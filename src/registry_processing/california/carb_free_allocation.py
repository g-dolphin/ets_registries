"""CARB (California Cap-and-Trade) — estimate facility-level free allocation.

CARB publishes:
  * facility-level GHG emissions under the Mandatory Reporting Regulation (MRR)
  * sector-aggregated allowance allocation totals via program data dashboards / summaries

CARB generally does *not* publish facility-by-facility free allocation totals, because
allocation is output-based and output/activity data can be confidential.

This module implements a *reconstruction* that is useful for research and modelling.

Method implemented: "Option 3" (hybrid intensity-adjusted allocation)
---------------------------------------------------------------
Within each allocation sector s and year t, we allocate the published sector total
A_total[s,t] to facilities i using weights:

  w_i = E_i * AF[s,t] * (B_s / I_i)^alpha

where
  * E_i      = facility covered emissions (tCO2e)
  * AF[s,t]  = (optional) assistance factor (unitless). If you do not provide it,
               AF defaults to 1 and cancels out within-sector.
  * B_s      = benchmark intensity for the sector (tCO2e per unit output).
               In California the official benchmarks are product-specific (Table 9-1).
               If you do not have product output, you can use a sector proxy benchmark.
  * I_i      = observed emissions intensity for the facility, if you have it.
               If you do not have output/intensity, set I_i = B_s (i.e., ratio=1).
  * alpha    = [0,1] controls how strongly intensity differences matter.

Then allocate:

  A_hat[i,t] = A_total[s,t] * w_i / sum_{j in s} w_j

This guarantees sums match CARB sector totals by construction.

Note: If you provide facility-specific intensity, this method behaves like an
output-based allocation proxy. If you don't, it reduces to proportional-to-emissions
within sector.

Outputs
-------
A tidy facility-year table with estimated free allocation and supporting fields.

"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Optional

import numpy as np
import pandas as pd


# ----------------------------
# Download URL helpers (public CARB files)
# ----------------------------

MRR_XLSX_URL_TEMPLATE = (
    "https://ww2.arb.ca.gov/sites/default/files/classic/cc/reporting/ghg-rep/reported-data/"
    "{year}-ghg-emissions-{release_date}.xlsx"
)

# The dashboard provides small CSVs for some vintages/years; CARB sometimes changes paths.
# These are best treated as *examples*; users can override URLs.
DEFAULT_ALLOCATION_SECTOR_CSV_URLS: Dict[int, str] = {
    2023: "https://ww2.arb.ca.gov/sites/default/files/2022-12/nc-allocation_v2023.csv",
}


# ----------------------------
# Parsing: CARB MRR spreadsheet
# ----------------------------

def read_mrr_facility_emissions_xlsx(
    path: str | Path,
    sheet_name: str = "2024 GHG Data",
    header_row: int = 7,
) -> pd.DataFrame:
    """Parse CARB 'GHG Facility and Entity Emissions' spreadsheet.

    Returns a facility/entity table with harmonized columns.

    Columns returned (when available):
      - report_year
      - arb_id
      - facility_name
      - industry_sector
      - naics
      - total_covered_emissions
      - emitter_covered_emissions
      - fuel_supplier_covered_emissions
      - electricity_importer_covered_emissions
      - city, state, zip

    The CARB sheet format is fairly stable across years, but column labels can drift.
    """

    path = Path(path)
    df = pd.read_excel(path, sheet_name=sheet_name, header=header_row)

    # column finder (case-insensitive, tolerant to newlines)
    def find_col(cands: Iterable[str], required: bool = True) -> Optional[str]:
        cols = list(df.columns)
        norm = {re.sub(r"\s+", " ", str(c)).strip().lower(): c for c in cols}
        for cand in cands:
            key = re.sub(r"\s+", " ", cand).strip().lower()
            if key in norm:
                return norm[key]
        if required:
            raise KeyError(f"Could not find any of columns {list(cands)}. Available: {cols}")
        return None

    c_arb = find_col(["ARB ID", "ARB ID "])
    c_name = find_col(["Facility Name", "Facility\nName"], required=False)
    c_year = find_col(["Report\nYear", "Report Year", "Year"], required=False)
    c_sector = find_col(["Industry Sector"], required=False)
    c_naics = find_col(["North American Industry Classification System (NAICS) \nCode and Description",
                        "North American Industry Classification System (NAICS) Code and Description"], required=False)

    c_total_cov = find_col(["Total Covered Emissions"], required=False)
    c_emit_cov = find_col(["Emitter Covered\nEmissions", "Emitter Covered Emissions"], required=False)
    c_fs_cov = find_col(["Fuel Supplier Covered\nEmissions", "Fuel Supplier Covered Emissions"], required=False)
    c_ei_cov = find_col(["Electricity Importer Covered Emissions"], required=False)

    c_city = find_col(["City"], required=False)
    c_state = find_col(["State"], required=False)
    c_zip = find_col(["Zip Code", "Zip"], required=False)

    out = pd.DataFrame({
        "arb_id": pd.to_numeric(df[c_arb], errors="coerce").astype("Int64"),
    })
    if c_year is not None:
        out["report_year"] = pd.to_numeric(df[c_year], errors="coerce").astype("Int64")
    if c_name is not None:
        out["facility_name"] = df[c_name].astype(str)
    if c_sector is not None:
        out["industry_sector"] = df[c_sector].astype(str)
    if c_naics is not None:
        out["naics"] = df[c_naics].astype(str)

    for colname, c in [
        ("total_covered_emissions", c_total_cov),
        ("emitter_covered_emissions", c_emit_cov),
        ("fuel_supplier_covered_emissions", c_fs_cov),
        ("electricity_importer_covered_emissions", c_ei_cov),
    ]:
        if c is not None:
            out[colname] = pd.to_numeric(df[c], errors="coerce")

    for colname, c in [("city", c_city), ("state", c_state), ("zip", c_zip)]:
        if c is not None:
            out[colname] = df[c].astype(str)

    # keep rows that look like real records
    out = out[out["arb_id"].notna()].copy()
    return out


def _guess_mrr_sheet_name(path: str | Path, year: int | None = None) -> str:
    """Best-effort inference of the "data" sheet name.

    CARB MRR annual spreadsheets use slightly different sheet names across years
    (e.g. "2024 GHG Data", "2013 GHG Data", or generic names).
    """

    xl = pd.ExcelFile(path)
    sheets = list(xl.sheet_names)
    if not sheets:
        raise ValueError(f"No sheets found in {Path(path).name}")

    # common pattern: "YYYY GHG Data"
    if year is not None:
        target = f"{year} GHG Data"
        if target in sheets:
            return target

    # otherwise: any sheet containing "GHG" and "Data"
    for s in sheets:
        low = s.lower()
        if "ghg" in low and "data" in low:
            return s

    # fallback: first sheet
    return sheets[0]


def discover_mrr_files(mrr_raw_dir: str | Path) -> list[Path]:
    """Discover all CARB MRR spreadsheets in a directory."""
    mrr_raw_dir = Path(mrr_raw_dir)
    files = sorted(
        list(mrr_raw_dir.glob("*.xlsx"))
        + list(mrr_raw_dir.glob("*.xls"))
        + list(mrr_raw_dir.glob("*.XLSX"))
        + list(mrr_raw_dir.glob("*.XLS"))
    )
    return files


def read_mrr_directory(
    mrr_raw_dir: str | Path,
    keep_source_file: bool = True,
) -> pd.DataFrame:
    """Read *all* CARB MRR annual spreadsheets in a directory and concatenate.

    Designed for a `_raw/california/` folder containing one file per year.
    The sheet name and header row differ slightly across years; this function
    uses heuristics and falls back gracefully.
    """

    files = discover_mrr_files(mrr_raw_dir)
    if not files:
        raise FileNotFoundError(f"No .xls/.xlsx files found in {Path(mrr_raw_dir).resolve()}")

    frames: list[pd.DataFrame] = []
    failed: list[tuple[str, str]] = []
    for f in files:
        # best-effort year inference from filename stem (e.g. '2024-ghg-emissions-...')
        m = re.search(r"(19\d{2}|20\d{2})", f.stem)
        inferred_year = int(m.group(1)) if m else None

        sheet = _guess_mrr_sheet_name(f, inferred_year)
        # header rows can drift; try a small set
        last_err: Optional[Exception] = None
        df: Optional[pd.DataFrame] = None
        # Header rows drift across years. In many older files, column headers start
        # around row 8 (0-indexed) on the "YYYY GHG Data" tab.
        for header in (10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0):
            try:
                df = read_mrr_facility_emissions_xlsx(f, sheet_name=sheet, header_row=header)
                break
            except Exception as e:  # noqa: BLE001
                last_err = e
                df = None
        if df is None:
            failed.append((f.name, str(last_err)))
            continue

        if "report_year" not in df.columns or df["report_year"].isna().all():
            if inferred_year is not None:
                df["report_year"] = inferred_year

        if keep_source_file:
            df["source_file"] = f.name

        frames.append(df)

    if not frames:
        raise RuntimeError(
            f"Failed to parse any CARB MRR files in {Path(mrr_raw_dir).resolve()}. "
            f"Last error: {failed[-1][1] if failed else 'unknown'}"
        )

    if failed:
        # Non-fatal: some older years may have slightly different schemas.
        print(f"[CARB MRR] Skipped {len(failed)} files that could not be parsed. Example: {failed[0][0]}")

    if not frames:
        raise RuntimeError(
            f"Failed to parse any MRR spreadsheets in {Path(mrr_raw_dir).resolve()}. "
            f"First error: {failed[0] if failed else 'unknown'}"
        )

    out = pd.concat(frames, ignore_index=True)
    if "report_year" in out.columns:
        out["report_year"] = pd.to_numeric(out["report_year"], errors="coerce").astype("Int64")
    return out


# ----------------------------
# Parsing: CARB allocation totals (sector-aggregated)
# ----------------------------

def read_allocation_sector_totals_csv(path: str | Path) -> pd.DataFrame:
    """Read CARB 'NC-ALLOCATION_VYYYY.CSV' style sector totals.

    Expected columns (as seen in public files):
      Sector, Vintage, Allocated Allowances, True-Up Value, Facilities

    Returns:
      vintage_year, sector, allocated_allowances, true_up_value, total_allocation
    """

    df = pd.read_csv(path)

    def find_col(name: str) -> str:
        for c in df.columns:
            if str(c).strip().lower() == name.strip().lower():
                return c
        raise KeyError(f"Missing column {name}. Available: {list(df.columns)}")

    c_sector = find_col("Sector")
    c_vintage = find_col("Vintage")
    c_alloc = find_col("Allocated Allowances")
    c_true = find_col("True-Up Value")

    out = pd.DataFrame({
        "vintage_year": pd.to_numeric(df[c_vintage], errors="coerce").astype("Int64"),
        "sector": df[c_sector].astype(str).str.strip(),
        "allocated_allowances": (
            df[c_alloc].astype(str).str.replace(",", "", regex=False).replace({"": np.nan}).astype(float)
        ),
        "true_up_value": (
            df[c_true].astype(str).str.replace(",", "", regex=False)
            .replace({"": np.nan, "nan": np.nan}).astype(float)
        ),
    })

    out["total_allocation"] = out["allocated_allowances"].fillna(0.0) + out["true_up_value"].fillna(0.0)
    return out


# ----------------------------
# Sector mapping
# ----------------------------

DEFAULT_SECTOR_MAP: list[tuple[str, str]] = [
    # (regex pattern, CARB allocation sector)
    (r"refin", "Refining and Hydrogen Production"),
    (r"hydrogen", "Refining and Hydrogen Production"),
    (r"cement|lime|gypsum|clay", "Cement, Lime, Clay, Gypsum"),
    (r"oil and gas|extraction|production", "Oil and Gas Production"),
]


def map_to_allocation_sector(
    df: pd.DataFrame,
    source_col: str = "industry_sector",
    mapping: Optional[list[tuple[str, str]]] = None,
    default_sector: str = "Other",
) -> pd.Series:
    """Map a CARB MRR 'Industry Sector' string to a small number of allocation sectors.

    The mapping here is intentionally simple and should be adapted as you learn more
    about how you want to group CA facilities.
    """
    if mapping is None:
        mapping = DEFAULT_SECTOR_MAP

    src = df[source_col].fillna("").astype(str)
    out = pd.Series([default_sector] * len(df), index=df.index, dtype="object")
    for pat, sector in mapping:
        mask = src.str.contains(pat, flags=re.IGNORECASE, regex=True)
        out.loc[mask] = sector
    return out


# ----------------------------
# Option 3 estimator
# ----------------------------

@dataclass
class Option3Config:
    year: int
    alpha: float = 0.5
    # Column names in the facility dataframe
    facility_id_col: str = "arb_id"
    emissions_col: str = "total_covered_emissions"
    sector_col: str = "allocation_sector"
    # Optional observed intensity column (tCO2e per unit output)
    observed_intensity_col: Optional[str] = None
    # If no observed intensity, we set I_i = benchmark (ratio=1)



def estimate_free_allocation_option3(
    facilities: pd.DataFrame,
    sector_totals: pd.DataFrame,
    benchmarks: pd.DataFrame,
    assistance_factors: Optional[pd.DataFrame] = None,
    config: Optional[Option3Config] = None,
) -> pd.DataFrame:
    """Estimate facility-level free allocation using Option 3.

    Parameters
    ----------
    facilities:
        Facility table with at least:
          - facility id (ARB ID)
          - covered emissions
          - allocation sector

    sector_totals:
        Table with at least:
          - vintage_year (or year)
          - sector
          - total_allocation

    benchmarks:
        Table with at least:
          - sector
          - benchmark_intensity

    assistance_factors (optional):
        Table with at least:
          - year
          - sector
          - assistance_factor
        If omitted, assistance factor is treated as 1.

    Returns
    -------
    Facility-year table including estimated allocation.
    """

    if config is None:
        raise ValueError("config is required")
    if not (0.0 <= config.alpha <= 1.0):
        raise ValueError("alpha must be in [0,1]")

    f = facilities.copy()
    # Ensure required cols exist
    for c in [config.facility_id_col, config.emissions_col, config.sector_col]:
        if c not in f.columns:
            raise KeyError(f"facilities missing required column '{c}'")

    # benchmark join
    b = benchmarks.copy()
    if "sector" not in b.columns or "benchmark_intensity" not in b.columns:
        raise KeyError("benchmarks must contain columns: sector, benchmark_intensity")

    f = f.merge(b[["sector", "benchmark_intensity"]], left_on=config.sector_col, right_on="sector", how="left")
    f = f.drop(columns=["sector"])

    if f["benchmark_intensity"].isna().any():
        missing = f.loc[f["benchmark_intensity"].isna(), config.sector_col].unique().tolist()
        raise ValueError(f"Missing benchmark_intensity for sectors: {missing}")

    # assistance factors join (optional)
    if assistance_factors is not None:
        af = assistance_factors.copy()
        for c in ["year", "sector", "assistance_factor"]:
            if c not in af.columns:
                raise KeyError("assistance_factors must contain columns: year, sector, assistance_factor")
        af = af[af["year"] == config.year].copy()
        f = f.merge(
            af[["sector", "assistance_factor"]],
            left_on=config.sector_col,
            right_on="sector",
            how="left",
        ).drop(columns=["sector"])
    else:
        f["assistance_factor"] = 1.0

    f["assistance_factor"] = pd.to_numeric(f["assistance_factor"], errors="coerce").fillna(1.0)

    # observed intensity (optional)
    if config.observed_intensity_col is not None:
        if config.observed_intensity_col not in f.columns:
            raise KeyError(f"facilities missing observed_intensity_col '{config.observed_intensity_col}'")
        f["observed_intensity"] = pd.to_numeric(f[config.observed_intensity_col], errors="coerce")
    else:
        f["observed_intensity"] = np.nan

    # If observed intensity missing, set it equal to benchmark (ratio=1)
    f["observed_intensity"] = f["observed_intensity"].fillna(f["benchmark_intensity"])

    # weights
    E = pd.to_numeric(f[config.emissions_col], errors="coerce").fillna(0.0)
    ratio = (f["benchmark_intensity"] / f["observed_intensity"]).replace([np.inf, -np.inf], np.nan).fillna(1.0)
    # Protect against negative/zero intensities
    ratio = ratio.clip(lower=0.0, upper=1000.0)

    f["weight"] = E * f["assistance_factor"] * (ratio ** config.alpha)

    # sector totals for the target vintage year
    st = sector_totals.copy()
    if "vintage_year" not in st.columns:
        raise KeyError("sector_totals must contain 'vintage_year'")
    for c in ["sector", "total_allocation"]:
        if c not in st.columns:
            raise KeyError("sector_totals must contain 'sector' and 'total_allocation'")

    st = st[st["vintage_year"] == config.year].copy()
    if st.empty:
        raise ValueError(f"No sector totals found for vintage/year {config.year}")

    # allocate within sector
    f["year"] = config.year

    # sum weights per sector
    sums = f.groupby(config.sector_col, dropna=False)["weight"].sum().rename("sector_weight_sum")
    f = f.join(sums, on=config.sector_col)

    # merge sector total allocation
    f = f.merge(
        st[["sector", "total_allocation"]],
        left_on=config.sector_col,
        right_on="sector",
        how="left",
    ).drop(columns=["sector"])

    if f["total_allocation"].isna().any():
        missing = f.loc[f["total_allocation"].isna(), config.sector_col].unique().tolist()
        raise ValueError(f"Missing sector total allocation for sectors: {missing}")

    # compute allocated share
    f["allocation_share"] = np.where(
        f["sector_weight_sum"] > 0,
        f["weight"] / f["sector_weight_sum"],
        0.0,
    )

    f["estimated_free_allocation"] = f["total_allocation"] * f["allocation_share"]

    # final tidy output
    out_cols = [
        config.facility_id_col,
        "facility_name" if "facility_name" in f.columns else None,
        "year",
        config.sector_col,
        config.emissions_col,
        "benchmark_intensity",
        "observed_intensity",
        "assistance_factor",
        "weight",
        "total_allocation",
        "allocation_share",
        "estimated_free_allocation",
    ]
    out_cols = [c for c in out_cols if c is not None]
    return f[out_cols].copy()


# ----------------------------
# Convenience: run end-to-end on local files
# ----------------------------

def run_option3_from_files(
    mrr_xlsx: str | Path,
    allocation_sector_csv: str | Path,
    benchmarks_csv: str | Path,
    year: int,
    observed_intensity_col: Optional[str] = None,
    alpha: float = 0.5,
    out_csv: Optional[str | Path] = None,
) -> pd.DataFrame:
    """End-to-end helper for a single year.

    Inputs are local files you downloaded (or that CI can download).
    """
    facilities = read_mrr_facility_emissions_xlsx(mrr_xlsx, sheet_name=f"{year} GHG Data", header_row=7)

    # Map to allocation sectors
    if "industry_sector" in facilities.columns:
        facilities["allocation_sector"] = map_to_allocation_sector(facilities, source_col="industry_sector")
    else:
        facilities["allocation_sector"] = "Other"

    sector_totals = read_allocation_sector_totals_csv(allocation_sector_csv)
    benchmarks = pd.read_csv(benchmarks_csv)

    cfg = Option3Config(
        year=year,
        alpha=alpha,
        observed_intensity_col=observed_intensity_col,
    )

    out = estimate_free_allocation_option3(
        facilities=facilities,
        sector_totals=sector_totals,
        benchmarks=benchmarks,
        assistance_factors=None,
        config=cfg,
    )

    if out_csv is not None:
        out_csv = Path(out_csv)
        out_csv.parent.mkdir(parents=True, exist_ok=True)
        out.to_csv(out_csv, index=False)

    return out


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Estimate CARB facility-level free allocation (Option 3)")
    p.add_argument("--mrr-xlsx", required=True, help="Path to CARB MRR facility emissions xlsx")
    p.add_argument("--allocation-sector-csv", required=True, help="Path to CARB sector totals csv (NC-ALLOCATION_VYYYY.CSV)")
    p.add_argument("--benchmarks-csv", required=True, help="Path to sector benchmark intensity csv")
    p.add_argument("--year", type=int, required=True, help="Vintage/year to estimate")
    p.add_argument("--alpha", type=float, default=0.5, help="Intensity adjustment strength in [0,1]")
    p.add_argument("--observed-intensity-col", default=None, help="Optional column in MRR table to use as observed intensity")
    p.add_argument("--out", default=None, help="Output CSV path")

    args = p.parse_args()

    df = run_option3_from_files(
        mrr_xlsx=args.mrr_xlsx,
        allocation_sector_csv=args.allocation_sector_csv,
        benchmarks_csv=args.benchmarks_csv,
        year=args.year,
        observed_intensity_col=args.observed_intensity_col,
        alpha=args.alpha,
        out_csv=args.out,
    )

    print(df.head(10).to_string(index=False))
