"""UK ETS facility-year ingestion + harmonisation.

Inputs:
  * Allocation table (GOV.UK) -> parsed by :func:`read_allocation_table`.
  * Compliance report (public registry report) -> parsed by :func:`read_compliance_report`.

Outputs:
  Harmonised facility-year dataset for the integrated pipeline.

Allocation metrics:
  * Observed: allocation_total from the allocation table
  * Reconstructed: NaN (UK publishes observed installation allocations)
  * Counterfactual: Option 3 allocation using NACE sector totals computed from observed
    allocation and facility emissions.

Sector coding:
  UK source files do not reliably include NACE codes. The pipeline therefore supports
  an optional mapping file ``permit_id -> nace_code``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from .parse_allocation_table import read_allocation_table
from .parse_compliance_report import read_compliance_report
from .option3_reconstruct import option3_allocate, Option3Config


def read_ukets_facility_year(
    allocation_xlsx: Path,
    compliance_xlsx: Path,
    *,
    permit_to_nace_path: Optional[Path] = None,
    alpha_counterfactual: float = 0.5,
) -> pd.DataFrame:
    alloc = read_allocation_table(allocation_xlsx)
    comp = read_compliance_report(compliance_xlsx)

    alloc["permit_id"] = alloc["permit_id"].astype(str).str.strip()
    comp["permit_id"] = comp["permit_id"].astype(str).str.strip()

    df = pd.merge(alloc, comp, on=["permit_id", "year"], how="left")

    out = pd.DataFrame({
        "system_id": "ukets",
        "country_id": "GB",
        "year": pd.to_numeric(df["year"], errors="coerce").astype("Int64"),
        "facility_id": df["permit_id"],
        # Keep installation id if present in the allocation table.
        "installation_id": df.get("installation_id", np.nan),
        "facility_name": df.get("installation_name", np.nan),
        "operator_name": df.get("operator_name", np.nan),
        "naics_code": np.nan,
        "emissions_verified": pd.to_numeric(df.get("recorded_emissions"), errors="coerce"),
        "allowances_surrendered": pd.to_numeric(df.get("allowances_surrendered"), errors="coerce"),
        "allocation_observed_free": pd.to_numeric(df.get("allocation_total"), errors="coerce"),
    })

    # If the compliance report includes NACE details, use them as defaults.
    if "nace_code" in df.columns:
        out["nace_code"] = df["nace_code"].replace({"nan": np.nan, "None": np.nan})
    if "nace_description" in df.columns:
        out["nace_description"] = df["nace_description"].replace({"nan": np.nan, "None": np.nan})

    # Optional NACE mapping
    if permit_to_nace_path is not None and Path(permit_to_nace_path).exists():
        m = pd.read_csv(permit_to_nace_path)
        m["permit_id"] = m["permit_id"].astype(str).str.strip()
        # expected: permit_id, nace_code (+ optional nace_description, isic4_code)
        out = out.merge(m, left_on="facility_id", right_on="permit_id", how="left", suffixes=("", "_map"))
        out = out.drop(columns=["permit_id"], errors="ignore")
        # Prefer mapped codes if provided
        if "nace_code_map" in out.columns:
            out["nace_code"] = out["nace_code_map"].combine_first(out.get("nace_code"))
            out = out.drop(columns=["nace_code_map"], errors="ignore")
        if "nace_description_map" in out.columns:
            out["nace_description"] = out["nace_description_map"].combine_first(out.get("nace_description"))
            out = out.drop(columns=["nace_description_map"], errors="ignore")
        if "isic4_code" not in out.columns and "isic4_code_map" in out.columns:
            out["isic4_code"] = out["isic4_code_map"]
        out = out.drop(columns=["isic4_code_map"], errors="ignore")
    else:
        if "nace_code" not in out.columns:
            out["nace_code"] = np.nan
        if "nace_description" not in out.columns:
            out["nace_description"] = np.nan
        if "isic4_code" not in out.columns:
            out["isic4_code"] = np.nan

    # Counterfactual allocation: Option 3 within (year, nace_code) using sector totals
    # computed from observed allocation.
    sector_totals = (
        out.groupby(["year", "nace_code"], dropna=False)["allocation_observed_free"]
        .sum(min_count=1)
        .reset_index()
        .rename(columns={"allocation_observed_free": "sector_total_free_allocation"})
    )
    tmp = out.merge(sector_totals, on=["year", "nace_code"], how="left")
    tmp["intensity_proxy"] = tmp["emissions_verified"] / tmp["allocation_observed_free"]
    tmp["sector_key"] = tmp["nace_code"].astype(str)

    tmp = option3_allocate(
        tmp,
        sector_col="sector_key",
        emissions_col="emissions_verified",
        sector_total_col="sector_total_free_allocation",
        benchmark_col=None,
        intensity_col="intensity_proxy",
        cfg=Option3Config(alpha=alpha_counterfactual),
        out_col="allocation_counterfactual_free",
    )
    out["allocation_counterfactual_free"] = tmp["allocation_counterfactual_free"]

    out["allocation_reconstructed_free"] = np.nan

    out["allocation_observed_source"] = "GOV.UK allocation table"
    out["allocation_reconstructed_source"] = np.nan
    out["allocation_counterfactual_source"] = f"Option3(alpha={alpha_counterfactual}) on NACE totals"

    return out
