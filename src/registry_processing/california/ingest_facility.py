"""California Cap-and-Trade facility-year ingestion + harmonisation.

Inputs:
  * CARB MRR facility emissions spreadsheet (installation/entity-level)
  * CARB sector allocation totals CSV (NC-ALLOCATION_VYYYY.csv)

Outputs:
  Harmonised facility-year dataset for the integrated pipeline.

Allocation metrics:
  * Observed: NaN (CARB does not publish facility-level allocations)
  * Reconstructed: Option 3 estimate of facility allocations matching sector totals
  * Counterfactual: Option 3 with alpha=0 (proportional-to-emissions within sector)

Sector coding:
  CARB provides NAICS (often as code + description). We apply a first-pass
  NAICS→NACE/ISIC concordance focused on ETS sectors.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from ..harmonize import map_naics_to_nace_isic
from .carb_free_allocation import (
    read_mrr_facility_emissions_xlsx,
    read_mrr_directory,
    read_allocation_sector_totals_csv,
    map_to_allocation_sector,
    estimate_free_allocation_option3,
    Option3Config,
)


def _default_benchmarks_for(sectors: pd.Series) -> pd.DataFrame:
    # Benchmarks are used only as ratios; if no observed intensity is available,
    # any positive constant works because B/I becomes 1.
    s = pd.Series(sectors.dropna().unique()).astype(str)
    return pd.DataFrame({"sector": s, "benchmark_intensity": 1.0})


def read_carb_facility_year(
    mrr_xlsx: Path,
    allocation_sector_csv: Path,
    *,
    sheet_name: Optional[str] = None,
    alpha_counterfactual: float = 0.5,
) -> pd.DataFrame:
    # Try to infer sheet name if not provided: pick the first sheet.
    # (CARB files are often like '2024 GHG Data')
    if sheet_name is None:
        xl = pd.ExcelFile(mrr_xlsx)
        sheet_name = xl.sheet_names[0]

    # Derive year from sheet name if possible
    year = None
    for tok in str(sheet_name).split():
        if tok.isdigit() and len(tok) == 4:
            year = int(tok)
            break

    facilities = read_mrr_facility_emissions_xlsx(mrr_xlsx, sheet_name=sheet_name, header_row=7)
    # Prefer facility sources (emitters) but fall back to total covered.
    emissions_col = "total_covered_emissions"
    if "emitter_covered_emissions" in facilities.columns:
        emissions_col = "emitter_covered_emissions"

    # Map to CARB allocation sectors
    if "industry_sector" in facilities.columns:
        facilities["allocation_sector"] = map_to_allocation_sector(facilities, source_col="industry_sector")
    else:
        facilities["allocation_sector"] = "Other"

    sector_totals = read_allocation_sector_totals_csv(allocation_sector_csv)
    # If year not inferred, use the latest vintage year in the totals file.
    if year is None:
        year = int(pd.to_numeric(sector_totals["vintage_year"], errors="coerce").max())

    benchmarks = _default_benchmarks_for(facilities["allocation_sector"])

    # Reconstructed (Option 3, alpha=alpha_counterfactual)
    cfg_rec = Option3Config(
        year=year,
        alpha=float(alpha_counterfactual),
        facility_id_col="arb_id",
        emissions_col=emissions_col,
        sector_col="allocation_sector",
        observed_intensity_col=None,
    )
    rec = estimate_free_allocation_option3(
        facilities=facilities,
        sector_totals=sector_totals,
        benchmarks=benchmarks,
        assistance_factors=None,
        config=cfg_rec,
    )

    # Counterfactual (alpha=0 -> proportional to emissions within sector)
    cfg_cf = Option3Config(
        year=year,
        alpha=0.0,
        facility_id_col="arb_id",
        emissions_col=emissions_col,
        sector_col="allocation_sector",
        observed_intensity_col=None,
    )
    cf = estimate_free_allocation_option3(
        facilities=facilities,
        sector_totals=sector_totals,
        benchmarks=benchmarks,
        assistance_factors=None,
        config=cfg_cf,
    )

    out = pd.DataFrame({
        "system_id": "caccat",
        "country_id": "US-CA",
        "year": year,
        "facility_id": rec["arb_id"],
        "facility_name": rec.get("facility_name", np.nan),
        "operator_name": np.nan,
        "naics_code": facilities.get("naics", np.nan),
        "emissions_verified": pd.to_numeric(rec[emissions_col], errors="coerce"),
        "allowances_surrendered": np.nan,
        "allocation_observed_free": np.nan,
        "allocation_reconstructed_free": pd.to_numeric(rec["estimated_free_allocation"], errors="coerce"),
        "allocation_counterfactual_free": pd.to_numeric(cf["estimated_free_allocation"], errors="coerce"),
        "allocation_observed_source": np.nan,
        "allocation_reconstructed_source": f"Option3(alpha={alpha_counterfactual}) on CARB sector totals",
        "allocation_counterfactual_source": "Option3(alpha=0) on CARB sector totals",
    })

    # Harmonise to NACE/ISIC via NAICS crosswalk
    nace_map = map_naics_to_nace_isic(pd.Series(out["naics_code"]))
    out = pd.concat([out.reset_index(drop=True), nace_map.reset_index(drop=True)], axis=1)

    return out


def read_carb_facility_years(
    mrr_raw_dir: Path,
    allocation_sector_csv: Path,
    *,
    alpha_counterfactual: float = 0.5,
    notify_missing_years: bool = True,
) -> pd.DataFrame:
    """Read all CARB MRR annual files from a directory and return a stacked
    facility-year table.

    The directory should contain one MRR file per year (as downloaded from CARB's
    MRR data page). The helper parses all spreadsheets, infers `report_year`,
    and then computes reconstructed and counterfactual allocation per year.
    """

    facilities_all = read_mrr_directory(mrr_raw_dir, keep_source_file=True)
    if "report_year" not in facilities_all.columns:
        raise ValueError("Could not infer `report_year` from CARB MRR files.")

    years = sorted([int(y) for y in facilities_all["report_year"].dropna().unique()])
    frames: list[pd.DataFrame] = []
    for y in years:
        sub = facilities_all[facilities_all["report_year"] == y].copy()
        # Build a temporary xlsx-like frame interface by using the same estimator code paths.
        # We reuse the allocation logic from `read_carb_facility_year` but avoid re-reading
        # the files.
        emissions_col = "total_covered_emissions"
        if "emitter_covered_emissions" in sub.columns:
            emissions_col = "emitter_covered_emissions"

        if "industry_sector" in sub.columns:
            sub["allocation_sector"] = map_to_allocation_sector(sub, source_col="industry_sector")
        else:
            sub["allocation_sector"] = "Other"

        sector_totals = read_allocation_sector_totals_csv(allocation_sector_csv)
        # Try matching vintage year to report year; fallback to latest.
        sector_totals_y = sector_totals[sector_totals["vintage_year"] == y]
        if sector_totals_y.empty:
            if notify_missing_years:
                print(f"[CARB] No sector totals found for vintage/year {y}; skipping.")
            continue

        benchmarks = _default_benchmarks_for(sub["allocation_sector"])

        cfg_rec = Option3Config(
            year=y,
            alpha=float(alpha_counterfactual),
            facility_id_col="arb_id",
            emissions_col=emissions_col,
            sector_col="allocation_sector",
            observed_intensity_col=None,
        )
        rec = estimate_free_allocation_option3(
            facilities=sub,
            sector_totals=sector_totals_y,
            benchmarks=benchmarks,
            assistance_factors=None,
            config=cfg_rec,
        )

        cfg_cf = Option3Config(
            year=y,
            alpha=0.0,
            facility_id_col="arb_id",
            emissions_col=emissions_col,
            sector_col="allocation_sector",
            observed_intensity_col=None,
        )
        cf = estimate_free_allocation_option3(
            facilities=sub,
            sector_totals=sector_totals_y,
            benchmarks=benchmarks,
            assistance_factors=None,
            config=cfg_cf,
        )

        out = pd.DataFrame({
            "system_id": "caccat",
            "country_id": "US-CA",
            "year": y,
            "facility_id": rec["arb_id"],
            "facility_name": rec.get("facility_name", np.nan),
            "operator_name": np.nan,
            "naics_code": sub.get("naics", np.nan),
            "emissions_verified": pd.to_numeric(rec[emissions_col], errors="coerce"),
            "allowances_surrendered": np.nan,
            "allocation_observed_free": np.nan,
            "allocation_reconstructed_free": pd.to_numeric(rec["estimated_free_allocation"], errors="coerce"),
            "allocation_counterfactual_free": pd.to_numeric(cf["estimated_free_allocation"], errors="coerce"),
            "allocation_observed_source": np.nan,
            "allocation_reconstructed_source": f"Option3(alpha={alpha_counterfactual}) on CARB sector totals",
            "allocation_counterfactual_source": "Option3(alpha=0) on CARB sector totals",
        })

        nace_map = map_naics_to_nace_isic(pd.Series(out["naics_code"]))
        out = pd.concat([out.reset_index(drop=True), nace_map.reset_index(drop=True)], axis=1)
        frames.append(out)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
