"""Schema harmonisation utilities for ETS registries.

This codebase aims to standardise outputs across:
  * EU ETS (EUTL processed data)
  * UK ETS (allocation table + public compliance report)
  * California Cap-and-Trade (CARB MRR + sector allocation totals)

The integrated pipeline produces two CSVs:
  1) facility-level dataset (one row per facility-year)
  2) sector-level dataset (one row per year x country/region x NACE code)

The sector-level output is aligned to the EU ETS aggregation schema (see
``euets_sector_nace_year.csv``).

Concordance
-----------
EU/UK datasets typically provide NACE directly (or can be mapped to it).
California provides NAICS; true NAICS→NACE concordances are detailed and
many-to-many. For a *working first pass* we provide a conservative crosswalk
focused on ETS-relevant heavy industry and power.

If you need higher fidelity, you can replace the crosswalk with an official
concordance (e.g. Eurostat/RAMON, UN, OECD) and keep the same interface.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

import numpy as np
import pandas as pd


SECTOR_COLS_EU_SCHEMA = [
    "reportedInSystem_id",
    "year",
    "country_id",
    "nace_code",
    "allocatedFree",
    "allocatedNewEntrance",
    "allocatedTotal",
    "allocated10c",
    "verified",
    "surrendered",
    "balance",
    "penalty",
    "free_share",
    "nace_level",
    "nace_description",
    "isic4_code",
]


@dataclass(frozen=True)
class ConcordanceRow:
    """One row in a first-pass concordance."""

    source_type: str  # e.g. 'NAICS', 'NACE', 'ISIC'
    source_prefix: str  # prefix match (string)
    nace_code: str
    nace_description: str
    isic4_code: str


def first_pass_concordance() -> pd.DataFrame:
    """Return a working first-pass concordance NAICS→(NACE, ISIC).

    The concordance is designed to be:
      * simple (prefix matching)
      * focused on ETS-heavy sectors
      * stable and transparent

    It is *not* intended to be a full official crosswalk.
    """

    rows = [
        # Power generation / utilities
        ConcordanceRow("NAICS", "2211", "35.1", "Electric power generation, transmission and distribution", "35"),
        ConcordanceRow("NAICS", "221", "35", "Electricity, gas, steam and air conditioning supply", "35"),

        # Refineries
        ConcordanceRow("NAICS", "32411", "19.2", "Manufacture of refined petroleum products", "19"),
        ConcordanceRow("NAICS", "324", "19", "Manufacture of coke and refined petroleum products", "19"),

        # Cement / minerals
        ConcordanceRow("NAICS", "32731", "23.5", "Manufacture of cement, lime and plaster", "23"),
        ConcordanceRow("NAICS", "3273", "23.5", "Manufacture of cement, lime and plaster", "23"),
        ConcordanceRow("NAICS", "327", "23", "Manufacture of other non-metallic mineral products", "23"),

        # Iron & steel
        ConcordanceRow("NAICS", "33111", "24.1", "Manufacture of basic iron and steel and of ferro-alloys", "24"),
        ConcordanceRow("NAICS", "3311", "24.1", "Manufacture of basic iron and steel and of ferro-alloys", "24"),
        ConcordanceRow("NAICS", "331", "24", "Manufacture of basic metals", "24"),

        # Chemicals
        ConcordanceRow("NAICS", "325", "20", "Manufacture of chemicals and chemical products", "20"),

        # Pulp & paper
        ConcordanceRow("NAICS", "322", "17", "Manufacture of paper and paper products", "17"),

        # Food (covered facilities sometimes)
        ConcordanceRow("NAICS", "311", "10", "Manufacture of food products", "10"),
    ]

    return pd.DataFrame([r.__dict__ for r in rows])


def map_naics_to_nace_isic(
    naics_series: pd.Series,
    concordance: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """Map NAICS codes (string-ish) to NACE + ISIC using prefix matching.

    Returns a DataFrame with columns: nace_code, nace_description, isic4_code.
    """
    if concordance is None:
        concordance = first_pass_concordance()

    conc = concordance[concordance["source_type"].str.upper() == "NAICS"].copy()
    # Longer prefixes should win.
    conc["_len"] = conc["source_prefix"].astype(str).str.len()
    conc = conc.sort_values("_len", ascending=False)

    def _norm_naics(x) -> str:
        if pd.isna(x):
            return ""
        s = str(x)
        # Common formatting: '32411 - Petroleum...' -> '32411'
        s = s.strip()
        # take leading digits
        digits = "".join(ch for ch in s if ch.isdigit())
        return digits

    naics_norm = naics_series.apply(_norm_naics)
    out = pd.DataFrame(index=naics_series.index, columns=["nace_code", "nace_description", "isic4_code"])
    out[:] = np.nan

    for _, r in conc.iterrows():
        pref = str(r["source_prefix"])
        mask = naics_norm.str.startswith(pref) & out["nace_code"].isna()
        out.loc[mask, "nace_code"] = r["nace_code"]
        out.loc[mask, "nace_description"] = r["nace_description"]
        out.loc[mask, "isic4_code"] = r["isic4_code"]

    return out


def add_validation_flags_facility(df: pd.DataFrame) -> pd.DataFrame:
    """Add non-blocking validation flags to a facility-year dataset."""
    out = df.copy()

    def _neg(x):
        return pd.notna(x) & (x < 0)

    # allocations
    for c in [
        "allocation_observed_free",
        "allocation_reconstructed_free",
        "allocation_counterfactual_free",
    ]:
        if c in out.columns:
            out[f"flag_{c}_negative"] = _neg(out[c])

    # surrenders vs emissions
    if "emissions_verified" in out.columns and "allowances_surrendered" in out.columns:
        tol = 1e-6
        out["flag_surrender_lt_emissions"] = (
            pd.notna(out["allowances_surrendered"]) & pd.notna(out["emissions_verified"]) &
            (out["allowances_surrendered"] + tol < out["emissions_verified"])
        )
    else:
        out["flag_surrender_lt_emissions"] = np.nan

    return out


def build_sector_output_from_facility(
    facility: pd.DataFrame,
    system_id: str,
    country_id: str,
    allocation_col: str,
    nace_code_col: str = "nace_code",
    emissions_col: str = "emissions_verified",
    surrendered_col: str = "allowances_surrendered",
) -> pd.DataFrame:
    """Aggregate a facility-year table to the EU sector schema."""
    df = facility.copy()
    df["reportedInSystem_id"] = system_id
    df["country_id"] = country_id

    # numeric
    for c in [allocation_col, emissions_col, surrendered_col]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    grp = (
        df.groupby(["reportedInSystem_id", "year", "country_id", nace_code_col], dropna=False)
        .agg(
            allocatedFree=(allocation_col, "sum"),
            verified=(emissions_col, "sum"),
            surrendered=(surrendered_col, "sum"),
        )
        .reset_index()
        .rename(columns={nace_code_col: "nace_code"})
    )

    grp["allocatedNewEntrance"] = np.nan
    grp["allocated10c"] = np.nan
    grp["allocatedTotal"] = grp["allocatedFree"]
    grp["balance"] = grp["allocatedTotal"] - grp["surrendered"]
    grp["penalty"] = np.nan
    grp["free_share"] = grp["allocatedFree"] / grp["verified"]

    # Keep optional descriptive columns if present
    if "nace_level" in df.columns or "nace_description" in df.columns or "isic4_code" in df.columns:
        meta = (
            df[[nace_code_col] + [c for c in ["nace_level", "nace_description", "isic4_code"] if c in df.columns]]
            .drop_duplicates()
            .rename(columns={nace_code_col: "nace_code"})
        )
        grp = grp.merge(meta, on="nace_code", how="left")
    else:
        grp["nace_level"] = np.nan
        grp["nace_description"] = np.nan
        grp["isic4_code"] = np.nan

    # Ensure schema order and presence
    for c in SECTOR_COLS_EU_SCHEMA:
        if c not in grp.columns:
            grp[c] = np.nan
    grp = grp[SECTOR_COLS_EU_SCHEMA]
    return grp
