"""End-to-end UK ETS pipeline (public data ingestion + optional Option 3).

Typical UK ETS use:
- Parse allocation table (ground truth free allocation)
- Parse compliance report (recorded emissions + surrenders)
- Merge on (permit_id, year)
- Compute intensity proxy (emissions / allocation_total) for heterogeneity diagnostics

Option 3 use (counterfactual/harmonization):
- Provide sector totals per year and a mapping from permit_id -> sector.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd

from .parse_allocation_table import read_allocation_table
from .parse_compliance_report import read_compliance_report
from .option3_reconstruct import option3_allocate, Option3Config


def merge_alloc_emissions(alloc: pd.DataFrame, comp: pd.DataFrame) -> pd.DataFrame:
    alloc = alloc.copy()
    comp = comp.copy()
    alloc["permit_id"] = alloc["permit_id"].astype(str).str.strip()
    comp["permit_id"] = comp["permit_id"].astype(str).str.strip()

    out = pd.merge(
        alloc,
        comp,
        on=["permit_id", "year"],
        how="left",
        suffixes=("_alloc", "_comp"),
    )
    return out


def add_intensity_proxy(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["intensity_proxy"] = out["recorded_emissions"] / out["allocation_total"]
    return out


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--allocation-xlsx", type=Path, required=True)
    p.add_argument("--compliance-xlsx", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)

    # Option 3 inputs
    p.add_argument("--permit-to-sector", type=Path, default=None,
                  help="CSV with columns: permit_id, sector")
    p.add_argument("--sector-totals", type=Path, default=None,
                  help="CSV with columns: year, sector, sector_total_free_allocation")
    p.add_argument("--sector-benchmarks", type=Path, default=None,
                  help="Optional CSV with columns: sector, benchmark_proxy")
    p.add_argument("--alpha", type=float, default=0.5)
    args = p.parse_args()

    alloc = read_allocation_table(args.allocation_xlsx)
    comp = read_compliance_report(args.compliance_xlsx)
    merged = merge_alloc_emissions(alloc, comp)
    merged = add_intensity_proxy(merged)

    # Optional Option 3 run
    if args.permit_to_sector and args.sector_totals:
        pts = pd.read_csv(args.permit_to_sector)
        st = pd.read_csv(args.sector_totals)
        merged = merged.merge(pts, on="permit_id", how="left")
        merged = merged.merge(st, on=["year", "sector"], how="left")

        if args.sector_benchmarks:
            bm = pd.read_csv(args.sector_benchmarks)
            merged = merged.merge(bm, on="sector", how="left")
            benchmark_col = "benchmark_proxy"
        else:
            benchmark_col = None

        merged = option3_allocate(
            merged,
            sector_col="sector",
            emissions_col="recorded_emissions",
            sector_total_col="sector_total_free_allocation",
            benchmark_col=benchmark_col,
            intensity_col="intensity_proxy",
            cfg=Option3Config(alpha=args.alpha),
        )

    args.out.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(args.out, index=False)
    print(f"Wrote {len(merged):,} rows -> {args.out}")


if __name__ == "__main__":
    main()
