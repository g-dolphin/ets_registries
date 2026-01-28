"""Integrated ETS registries pipeline.

Produces two harmonised outputs across systems:
  * facility_level.csv  (one row per facility-year per system)
  * sector_level.csv    (one row per year x country/region x NACE code per system)

The sector-level schema follows the EU ETS aggregation output
(``euets_sector_nace_year.csv``).

This pipeline is designed to be *non-interrupting*: validation checks generate
boolean flags rather than raising errors.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd

from .harmonize import (
    SECTOR_COLS_EU_SCHEMA,
    add_validation_flags_facility,
    build_sector_output_from_facility,
    build_isic3_output_from_sector,
)

from .euets.ingest_facility import read_euets_facility_year
from .euets.ingest_sector import read_euets_sector_nace_year
from .ukets.ingest_facility import read_ukets_facility_year
from .california.ingest_facility import read_carb_facility_year, read_carb_facility_years


def _ensure_cols(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c not in out.columns:
            out[c] = np.nan
    return out


FACILITY_SCHEMA = [
    "system_id",
    "country_id",
    "year",
    "facility_id",
    "installation_id",
    "facility_name",
    "operator_name",
    "nace_code",
    "nace_description",
    "isic4_code",
    "naics_code",
    "emissions_verified",
    "allowances_surrendered",
    # allocation metrics
    "allocation_observed_free",
    "allocation_reconstructed_free",
    "allocation_counterfactual_free",
    # provenance
    "allocation_observed_source",
    "allocation_reconstructed_source",
    "allocation_counterfactual_source",
]


def run_pipeline(
    eutl_dir: Path | None,
    uk_allocation_xlsx: Path | None,
    uk_compliance_xlsx: Path | None,
    uk_permit_to_nace: Path | None,
    ca_mrr_xlsx: Path | None,
    ca_mrr_raw_dir: Path | None,
    ca_mrr_sheet: str | None,
    ca_allocation_sector_csv: Path | None,
    out_facility: Path,
    out_sector: Path,
    out_sector_isic3: Path | None = None,
    alpha_counterfactual: float = 0.5,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    facility_frames: List[pd.DataFrame] = []

    if eutl_dir is not None:
        facility_frames.append(read_euets_facility_year(eutl_dir, alpha_counterfactual=alpha_counterfactual))
    if uk_allocation_xlsx is not None and uk_compliance_xlsx is not None:
        facility_frames.append(
            read_ukets_facility_year(
                uk_allocation_xlsx,
                uk_compliance_xlsx,
                permit_to_nace_path=uk_permit_to_nace,
                alpha_counterfactual=alpha_counterfactual,
            )
        )
    if ca_allocation_sector_csv is not None:
        if ca_mrr_raw_dir is not None:
            facility_frames.append(
                read_carb_facility_years(
                    ca_mrr_raw_dir,
                    ca_allocation_sector_csv,
                    alpha_counterfactual=alpha_counterfactual,
                )
            )
        elif ca_mrr_xlsx is not None:
            facility_frames.append(
                read_carb_facility_year(
                    ca_mrr_xlsx,
                    ca_allocation_sector_csv,
                    sheet_name=ca_mrr_sheet,
                    alpha_counterfactual=alpha_counterfactual,
                )
            )

    if not facility_frames:
        raise ValueError("No systems configured. Provide at least one input set.")

    facility = pd.concat(facility_frames, ignore_index=True)
    facility = _ensure_cols(facility, FACILITY_SCHEMA)
    facility = facility[FACILITY_SCHEMA + [c for c in facility.columns if c.startswith("flag_")]]

    # Add validation flags
    facility = add_validation_flags_facility(facility)

    # Sector outputs: build once per allocation metric and stack
    sector_frames: List[pd.DataFrame] = []
    for metric, col in [
        ("observed", "allocation_observed_free"),
        ("reconstructed", "allocation_reconstructed_free"),
        ("counterfactual", "allocation_counterfactual_free"),
    ]:
        tmp = facility.copy()
        # Add a metric column to allow consumers to select which allocation definition they want.
        tmp["allocation_metric"] = metric
        sec = []
        for system_id, country_id in (
            tmp[["system_id", "country_id"]].drop_duplicates().itertuples(index=False, name=None)
        ):
            # Preserve canonical EU schema for EU observed.
            if metric == "observed" and system_id == "euets" and eutl_dir is not None:
                eu_sec = read_euets_sector_nace_year(eutl_dir, system="euets").copy()
                eu_sec["allocation_metric"] = metric
                sec.append(eu_sec)
                continue

            sub = tmp[(tmp["system_id"] == system_id) & (tmp["country_id"] == country_id)].copy()
            sec_df = build_sector_output_from_facility(
                sub,
                system_id=system_id,
                country_id=country_id,
                allocation_col=col,
            )
            sec_df["allocation_metric"] = metric
            sec.append(sec_df)
        sector_frames.append(pd.concat(sec, ignore_index=True))

    sector = pd.concat(sector_frames, ignore_index=True)
    # Ensure EU schema + extra metric column
    for c in SECTOR_COLS_EU_SCHEMA:
        if c not in sector.columns:
            sector[c] = np.nan
    if "allocation_metric" not in sector.columns:
        sector["allocation_metric"] = "observed"
    sector = sector[SECTOR_COLS_EU_SCHEMA + ["allocation_metric"]]

    out_facility.parent.mkdir(parents=True, exist_ok=True)
    out_sector.parent.mkdir(parents=True, exist_ok=True)
    facility.to_csv(out_facility, index=False)
    sector.to_csv(out_sector, index=False)

    if out_sector_isic3 is not None:
        isic3_frames: List[pd.DataFrame] = []
        for metric in sector["allocation_metric"].dropna().unique():
            sec_metric = sector[sector["allocation_metric"] == metric].copy()
            isic3 = build_isic3_output_from_sector(sec_metric)
            isic3["allocation_metric"] = metric
            isic3_frames.append(isic3)
        if isic3_frames:
            isic3_out = pd.concat(isic3_frames, ignore_index=True)
            out_sector_isic3.parent.mkdir(parents=True, exist_ok=True)
            isic3_out.to_csv(out_sector_isic3, index=False)

    return facility, sector


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--eutl-dir", type=Path, default=None, help="EU ETS EUTL extracted directory")

    p.add_argument("--uk-allocation-xlsx", type=Path, default=None)
    p.add_argument("--uk-compliance-xlsx", type=Path, default=None)
    p.add_argument(
        "--uk-permit-to-nace",
        type=Path,
        default=None,
        help="Optional CSV mapping permit_id -> nace_code (+ optional descriptions)",
    )

    p.add_argument("--ca-mrr-xlsx", type=Path, default=None)
    p.add_argument("--ca-mrr-raw-dir", type=Path, default=None, help="Directory containing CARB MRR annual spreadsheets")
    p.add_argument("--ca-mrr-sheet", type=str, default=None)
    p.add_argument("--ca-allocation-sector-csv", type=Path, default=None)

    p.add_argument("--out-facility", type=Path, required=True)
    p.add_argument("--out-sector", type=Path, required=True)
    p.add_argument("--out-sector-isic3", type=Path, default=None)
    p.add_argument(
        "--alpha-counterfactual",
        type=float,
        default=0.5,
        help="Alpha used for Option-3 counterfactual allocation in all systems.",
    )
    args = p.parse_args()

    run_pipeline(
        eutl_dir=args.eutl_dir,
        uk_allocation_xlsx=args.uk_allocation_xlsx,
        uk_compliance_xlsx=args.uk_compliance_xlsx,
        uk_permit_to_nace=args.uk_permit_to_nace,
        ca_mrr_xlsx=args.ca_mrr_xlsx,
        ca_mrr_raw_dir=args.ca_mrr_raw_dir,
        ca_mrr_sheet=args.ca_mrr_sheet,
        ca_allocation_sector_csv=args.ca_allocation_sector_csv,
        out_facility=args.out_facility,
        out_sector=args.out_sector,
        out_sector_isic3=args.out_sector_isic3,
        alpha_counterfactual=args.alpha_counterfactual,
    )


if __name__ == "__main__":
    main()
