"""Washington Cap-and-Invest (Climate Commitment Act) facility-year ingestion + harmonisation.

Inputs:
  * WA GHG Reporting Program Public Publication export (CSV)
  * WA Ecology no-cost allowances PDFs (aggregated EITE subsector totals)

Outputs:
  Harmonised facility-year dataset for the integrated pipeline.

Notes:
  * Emissions data is facility/reporter level (no separate "installation id" field is provided).
    We build a stable `facility_id` as a short SHA1 hash of reporter+jurisdiction+city.
  * No facility-level free allocation totals are published. We reconstruct a best-effort estimate by
    distributing published *subsector totals* proportional to emissions for facilities whose NAICS codes
    match the NAICS patterns listed in the PDF factsheet.

The PDF factsheet explicitly says allocations are aggregated for confidentiality and provides the 2023 totals
for 5 EITE subsectors. (See: Pub No. 23-02-098.) 
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional
import hashlib
import re

import numpy as np
import pandas as pd

from .parsers.allowance_pdfs import parse_allocation_dir
from .wa_free_allocation import assign_eite_subsector_from_naics, allocate_proportional


def _make_facility_id(reporter: str, jurisdiction: str | None, city: str | None) -> str:
    key = "|".join([str(reporter or "").strip().lower(),
                    str(jurisdiction or "").strip().lower(),
                    str(city or "").strip().lower()])
    h = hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]
    return f"wa_{h}"


def read_wa_emissions_csv(emissions_csv: Path) -> pd.DataFrame:
    """Read and standardise WA emissions CSV into a canonical facility-year table."""
    emissions_csv = Path(emissions_csv)
    df = pd.read_csv(emissions_csv)

    # Normalise column names we rely on
    col = {c.strip(): c for c in df.columns}

    def req(name: str) -> str:
        if name not in col:
            raise KeyError(f"Expected column '{name}' not found in {emissions_csv.name}")
        return col[name]

    reporter = req("Reporter")
    year = req("Year")
    sector = req("Sector")
    subsector = req("Subsector")
    naics = req("Primary NAICS Code")
    city = req("City")
    county = req("County")
    jurisdiction = req("Jurisdiction")
    emissions = req("Reported Emissions (MTCO2e)")
    covered_emissions = col.get("Covered Emissions (MT CO2e)") or col.get("Covered Emissions (MTCO2e)")
    cca_status = col.get("CCA Status")
    location = col.get("Location")
    geo = col.get("Georeferenced Location")

    out = pd.DataFrame({
        "reporter": df[reporter].astype(str).str.strip(),
        "year": pd.to_numeric(df[year], errors="coerce").astype("Int64"),
        "sector": df[sector].astype(str).str.strip(),
        "subsector": df[subsector].astype(str).str.strip(),
        "naics_code": pd.to_numeric(df[naics], errors="coerce"),
        "city": df[city].astype(str).str.strip(),
        "county": df[county].astype(str).str.strip(),
        "jurisdiction": df[jurisdiction].astype(str).str.strip(),
        "cca_status": df[cca_status].astype(str).str.strip() if cca_status else pd.NA,
        "emissions_reported": pd.to_numeric(df[emissions], errors="coerce"),
        "emissions_covered": pd.to_numeric(df[covered_emissions], errors="coerce") if covered_emissions else np.nan,
        "location": df[location] if location else pd.NA,
        "georeferenced_location": df[geo] if geo else pd.NA,
    })

    out["facility_id"] = [
        _make_facility_id(r, j, c) for r, j, c in zip(out["reporter"], out["jurisdiction"], out["city"])
    ]

    # Candidate allocation subsector (EITE) based on NAICS patterns from Ecology factsheet
    out["allocation_subsector"] = out["naics_code"].apply(assign_eite_subsector_from_naics)

    return out


def read_wa_facility_year(
    emissions_csv: Path,
    allowances_pdf_dir: Optional[Path] = None,
    *,
    keep_only_covered_entities: bool = True,
) -> pd.DataFrame:
    """Create harmonised WA facility-year dataset for the integrated pipeline.

    Parameters
    ----------
    emissions_csv:
        Path to the WA open-data export CSV.
    allowances_pdf_dir:
        Directory containing Ecology no-cost allowance PDFs. If None, allocation fields remain NaN.
    keep_only_covered_entities:
        If True, keep only rows where `cca_status == 'Covered Entity'` (common interpretation for ETS scope).

    Returns
    -------
    pandas.DataFrame
        Canonical columns expected by the higher-level pipeline.
    """
    base = read_wa_emissions_csv(emissions_csv)

    if keep_only_covered_entities and "cca_status" in base.columns:
        base = base[base["cca_status"].str.lower().eq("covered entity")].copy()

    # Start harmonised output
    out = pd.DataFrame({
        "system": "washington_cca",
        "jurisdiction": "Washington",
        "facility_id": base["facility_id"],
        "facility_name": base["reporter"],
        "year": base["year"].astype("Int64"),
        "sector": base["sector"],
        "subsector": base["subsector"],
        "naics_code": base["naics_code"],
        "city": base["city"],
        "county": base["county"],
        "reported_emissions_mtco2e": base["emissions_reported"],
        "covered_emissions_mtco2e": base["emissions_covered"],
        "emissions_verified": base["emissions_reported"],  # best available
        "allowances_surrendered": np.nan,

        # Allocation fields (no facility-level observed in WA public docs)
        "allocation_observed_free": np.nan,
        "allocation_reconstructed_free": np.nan,
        "allocation_counterfactual_free": np.nan,
        "allocation_observed_source": np.nan,
        "allocation_reconstructed_source": np.nan,
        "allocation_counterfactual_source": np.nan,

        # Extra metadata
        "allocation_subsector": base["allocation_subsector"],
        "georeferenced_location": base["georeferenced_location"],
    })

    if allowances_pdf_dir is not None:
        sector_totals = parse_allocation_dir(Path(allowances_pdf_dir))
        if len(sector_totals):
            # For now, apply allocation totals only where allocation_year matches emissions year.
            alloc = allocate_proportional(
                facilities=out.rename(columns={"year": "year", "emissions_verified": "emissions_verified"}),
                subsector_totals=sector_totals,
            )

            out["allocation_reconstructed_free"] = pd.to_numeric(alloc["estimated_free_allocation"], errors="coerce")
            out["allocation_counterfactual_free"] = out["allocation_reconstructed_free"]
            out["allocation_reconstructed_source"] = (
                "Proportional-to-emissions reconstruction from Ecology aggregated EITE subsector totals (PDF)"
            )
            out["allocation_counterfactual_source"] = out["allocation_reconstructed_source"]

    return out
