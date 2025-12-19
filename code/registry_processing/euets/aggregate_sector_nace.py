"""
Aggregate EU ETS installation-level compliance data to NACE (Rev.2) sector level.

Inputs (from eutl_2024_202410.zip extracted folder):
- compliance.csv  (installation_id, year, verified, allocatedFree, allocatedTotal, ...)
- installation.csv (id, country_id, nace_id, ...)
- nace_code.csv (id, level, description, isic4_id)

Outputs:
- euets_sector_nace_year.csv (one row per year x country x NACE code)
"""

from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd
import numpy as np


NUM_COLS = [
    "allocatedFree",
    "allocatedNewEntrance",
    "allocatedTotal",
    "allocated10c",
    "verified",
    "surrendered",
    "balance",
    "penalty",
]


def format_nace_rev2(x) -> str | float:
    """Convert eutl 'nace_id' values (often floats) into NACE Rev.2 string codes.

    Examples:
      6.2  -> '06.2'
      1.11 -> '01.11'
      51.0 -> '51'
    """
    if pd.isna(x):
        return np.nan

    s = str(x).strip()
    if s == "":
        return np.nan

    # Normalize float-like strings (e.g., "6.200000") to trimmed decimals
    try:
        f = float(s)
        s = f"{f:.3f}".rstrip("0").rstrip(".")
    except ValueError:
        pass

    parts = s.split(".")
    if parts and parts[0].isdigit() and len(parts[0]) == 1:
        parts[0] = parts[0].zfill(2)

    return ".".join(parts)


def load_inputs(eutl_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    compliance_path = eutl_dir / "compliance.csv"
    installation_path = eutl_dir / "installation.csv"
    nace_code_path = eutl_dir / "nace_code.csv"

    if not compliance_path.exists():
        raise FileNotFoundError(compliance_path)
    if not installation_path.exists():
        raise FileNotFoundError(installation_path)
    if not nace_code_path.exists():
        raise FileNotFoundError(nace_code_path)

    comp = pd.read_csv(compliance_path, low_memory=False)
    inst = pd.read_csv(installation_path, low_memory=False, usecols=["id", "country_id", "nace_id"])
    nace = pd.read_csv(nace_code_path, low_memory=False)

    return comp, inst, nace


def aggregate(comp: pd.DataFrame, inst: pd.DataFrame, nace: pd.DataFrame) -> pd.DataFrame:
    inst = inst.copy()
    inst["nace_code"] = inst["nace_id"].apply(format_nace_rev2)

    merged = comp.merge(
        inst,
        left_on="installation_id",
        right_on="id",
        how="left",
        validate="m:1",
    )

    for c in NUM_COLS:
        if c in merged.columns:
            merged[c] = pd.to_numeric(merged[c], errors="coerce")

    grp = (
        merged.groupby(["reportedInSystem_id", "year", "country_id", "nace_code"], dropna=False)[NUM_COLS]
        .sum(min_count=1)
        .reset_index()
    )

    grp["free_share"] = grp["allocatedFree"] / grp["verified"]

    nace_map = nace.rename(
        columns={
            "id": "nace_code",
            "level": "nace_level",
            "description": "nace_description",
            "isic4_id": "isic4_code",
        }
    )[["nace_code", "nace_level", "nace_description", "isic4_code"]]

    out = grp.merge(nace_map, on="nace_code", how="left")

    return out


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--eutl-dir",
        type=Path,
        required=True,
        help="Path to extracted eutl_2024_202410 directory containing compliance.csv, installation.csv, nace_code.csv",
    )
    p.add_argument(
        "--out",
        type=Path,
        required=True,
        help="Output CSV path (e.g., ../_output/euets/euets_sector_nace_year.csv)",
    )
    p.add_argument(
        "--system",
        type=str,
        default="euets",
        help="Filter reportedInSystem_id (default: euets). Use 'all' to keep all systems in the dataset.",
    )
    args = p.parse_args()

    comp, inst, nace = load_inputs(args.eutl_dir)
    out = aggregate(comp, inst, nace)

    if args.system != "all":
        out = out[out["reportedInSystem_id"] == args.system].copy()

    args.out.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out, index=False)
    print(f"Wrote {len(out):,} rows to {args.out}")


if __name__ == "__main__":
    main()
