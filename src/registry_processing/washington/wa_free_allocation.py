"""Washington Cap-and-Invest — best-effort free allocation reconstruction.

Ecology publishes *subsector-aggregated* no-cost allowances for EITE industries in PDF factsheets.
Facility-level allocations are withheld, so we reconstruct a facility allocation by distributing the
published subsector totals across matched facilities proportional to their verified emissions.

This is *not official* allocation data, but can be useful for modelling and pipeline integration.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class WAAllocationConfig:
    facility_id_col: str = "facility_id"
    subsector_col: str = "allocation_subsector"
    emissions_col: str = "emissions_verified"
    year_col: str = "year"
    allocation_year_col: str = "allocation_year"
    subsector_total_col: str = "total_allowances"


def assign_eite_subsector_from_naics(naics: Optional[float | int | str]) -> Optional[str]:
    """Map NAICS codes into the Ecology EITE subsectors listed in the PDF.

    Mapping is based on the NAICS code sets/wildcards shown in the EITE factsheet.
    If `naics` is missing/unparseable, returns None.
    """
    if naics is None or (isinstance(naics, float) and np.isnan(naics)):
        return None
    s = str(int(float(naics))) if str(naics).replace('.', '', 1).isdigit() else str(naics)
    s = re.sub(r"\D", "", s)
    if not s:
        return None

    def starts(prefix: str) -> bool:
        return s.startswith(prefix)

    # Building Products, Electronics and Aerospace Manufacturing
    if s == "327420" or s == "334413" or starts("3364"):
        return "Building Product, Electronics and Aerospace Manufacturing"

    # Food Processing and Manufacturing
    if starts("3114") or starts("3115") or s == "311611" or s == "311991":
        return "Food Processing and Manufacturing"

    # Petroleum Refining and Chemical Manufacturing
    if s == "324110" or starts("3251") or s == "325311":
        return "Petroleum Refining and Chemical Manufacturing"

    # Pulp, Paper and Cement Manufacturing
    if starts("3221") or s == "327310":
        return "Pulp, Paper and Cement Manufacturing"

    # Steel, Aluminum, and Glass Manufacturing
    if starts("32721") or s == "331110" or s == "331221" or starts("33131"):
        return "Steel, Aluminum, and Glass Manufacturing"

    return None


import re  # placed after function docstring to keep imports grouped in file header


def allocate_proportional(
    facilities: pd.DataFrame,
    subsector_totals: pd.DataFrame,
    *,
    config: WAAllocationConfig = WAAllocationConfig(),
) -> pd.DataFrame:
    """Allocate subsector totals across facilities proportional to emissions.

    facilities must have:
      * facility id
      * year
      * allocation_subsector
      * emissions_verified

    subsector_totals must have:
      * allocation_year
      * subsector (matching allocation_subsector)
      * total_allowances
    """
    f = facilities.copy()

    # Guardrails
    for c in [config.facility_id_col, config.year_col, config.subsector_col, config.emissions_col]:
        if c not in f.columns:
            raise KeyError(f"Facilities missing required column: {c}")

    st = subsector_totals.rename(columns={
        "subsector": config.subsector_col,
        config.subsector_total_col: "_subsector_total",
    }).copy()

    # If allocation_year is missing, assume it applies to the same year (best effort)
    if config.allocation_year_col not in st.columns:
        st[config.allocation_year_col] = st[config.year_col] if config.year_col in st.columns else np.nan

    # Merge totals onto facility records
    f = f.merge(
        st[[config.allocation_year_col, config.subsector_col, "_subsector_total"]],
        how="left",
        left_on=[config.year_col, config.subsector_col],
        right_on=[config.allocation_year_col, config.subsector_col],
    )

    # Compute weights within each year/subsector
    grp = f.groupby([config.year_col, config.subsector_col], dropna=False)
    denom = grp[config.emissions_col].transform(lambda s: pd.to_numeric(s, errors="coerce").fillna(0.0).sum())
    numer = pd.to_numeric(f[config.emissions_col], errors="coerce").fillna(0.0)

    # Avoid divide-by-zero: if denom=0, allocate 0
    w = np.where(denom.to_numpy() > 0, numer.to_numpy() / denom.to_numpy(), 0.0)

    f["estimated_free_allocation"] = w * pd.to_numeric(f["_subsector_total"], errors="coerce").fillna(np.nan)

    # if subsector_total is missing, keep NaN rather than 0
    f.loc[f["_subsector_total"].isna(), "estimated_free_allocation"] = np.nan

    return f.drop(columns=["_subsector_total", config.allocation_year_col])
