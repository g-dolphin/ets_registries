"""EU ETS facility-year ingestion + counterfactual allocation.

Reads EUTL processed CSVs:
  * compliance.csv (installation_id, year, verified, surrendered, allocatedFree, ...)
  * installation.csv (id, country_id, nace_id)

Outputs a harmonised facility-year table compatible with the integrated pipeline.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from ..harmonize import map_naics_to_nace_isic  # for signature parity (not used)
from .aggregate_sector_nace import format_nace_rev2
from ..ukets.option3_reconstruct import option3_allocate, Option3Config


def read_euets_facility_year(eutl_dir: Path, alpha_counterfactual: float = 0.5) -> pd.DataFrame:
    """Return EU ETS facility-year in harmonised schema.

    Observed allocation is available (allocatedFree).
    Reconstructed allocation is not computed (NaN).
    Counterfactual allocation is computed by re-allocating sector totals using Option 3.
    """

    comp = pd.read_csv(Path(eutl_dir) / "compliance.csv", low_memory=False)
    inst = pd.read_csv(Path(eutl_dir) / "installation.csv", low_memory=False, usecols=["id", "country_id", "nace_id"])

    inst = inst.copy()
    inst["nace_code"] = inst["nace_id"].apply(format_nace_rev2)

    df = comp.merge(inst, left_on="installation_id", right_on="id", how="left", validate="m:1")

    out = pd.DataFrame({
        "system_id": "euets",
        "country_id": df.get("country_id"),
        "year": pd.to_numeric(df.get("year"), errors="coerce").astype("Int64"),
        "facility_id": df.get("installation_id"),
        "facility_name": df.get("installation_name", np.nan),
        "operator_name": df.get("operator_name", np.nan),
        "nace_code": df.get("nace_code"),
        "naics_code": np.nan,
        "emissions_verified": pd.to_numeric(df.get("verified"), errors="coerce"),
        "allowances_surrendered": pd.to_numeric(df.get("surrendered"), errors="coerce"),
        "allocation_observed_free": pd.to_numeric(df.get("allocatedFree"), errors="coerce"),
    })

    # Try to attach NACE→ISIC metadata if present in eutl_dir
    nace_code_path = Path(eutl_dir) / "nace_code.csv"
    if nace_code_path.exists():
        nace = pd.read_csv(nace_code_path, low_memory=False)
        nace_map = nace.rename(
            columns={
                "id": "nace_code",
                "level": "nace_level",
                "description": "nace_description",
                "isic4_id": "isic4_code",
            }
        )[["nace_code", "nace_level", "nace_description", "isic4_code"]]
        out = out.merge(nace_map, on="nace_code", how="left")
    else:
        out["nace_level"] = np.nan
        out["nace_description"] = np.nan
        out["isic4_code"] = np.nan

    # Derive counterfactual allocation:
    # 1) sector totals from observed allocation
    sector_totals = (
        out.groupby(["year", "country_id", "nace_code"], dropna=False)["allocation_observed_free"]
        .sum(min_count=1)
        .reset_index()
        .rename(columns={"allocation_observed_free": "sector_total_free_allocation"})
    )

    tmp = out.merge(sector_totals, on=["year", "country_id", "nace_code"], how="left")
    # Intensity proxy: emissions / observed allocation (when available)
    tmp["intensity_proxy"] = tmp["emissions_verified"] / tmp["allocation_observed_free"]
    tmp["sector_key"] = tmp["country_id"].astype(str) + "|" + tmp["nace_code"].astype(str)
    tmp["sector_total_keyed"] = tmp["sector_total_free_allocation"]

    tmp = option3_allocate(
        tmp,
        sector_col="sector_key",
        emissions_col="emissions_verified",
        sector_total_col="sector_total_keyed",
        benchmark_col=None,
        intensity_col="intensity_proxy",
        cfg=Option3Config(alpha=alpha_counterfactual),
        out_col="allocation_counterfactual_free",
    )

    out["allocation_reconstructed_free"] = np.nan
    out["allocation_counterfactual_free"] = tmp["allocation_counterfactual_free"]

    out["allocation_observed_source"] = "EUTL compliance.csv:allocatedFree"
    out["allocation_reconstructed_source"] = np.nan
    out["allocation_counterfactual_source"] = f"Option3(alpha={alpha_counterfactual}) on NACE totals"

    return out
