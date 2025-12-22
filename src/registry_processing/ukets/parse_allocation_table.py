"""Parse UK ETS allocation table (GOV.UK) to tidy long format.

Output columns:
- permit_id
- installation_name
- operator_name
- year
- allocation_total
- allocation_standard (optional)
- allocation_ner (optional)
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd


def _norm_col(c: str) -> str:
    return re.sub(r"\s+", " ", str(c)).strip().lower()


def _find_col(df: pd.DataFrame, patterns: List[str]) -> str | None:
    cols = list(df.columns)
    norm = {_norm_col(c): c for c in cols}
    for pat in patterns:
        r = re.compile(pat)
        for nc, orig in norm.items():
            if r.search(nc):
                return orig
    return None


def read_allocation_table(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=0)

    permit_col = _find_col(df, [r"^permit id$", r"permit\s*id"])
    if permit_col is None:
        raise ValueError("Could not find Permit ID column in allocation table.")

    # GOV.UK allocation table (Dec 2025) uses "Installation ID" and "Account Holder Name"
    inst_id_col = _find_col(df, [r"installation id", r"^installation\s*id$"])
    inst_col = _find_col(df, [r"installation name", r"^installation$"])
    op_col = _find_col(df, [r"operator name", r"account holder name", r"operator$"])

    # Year columns can appear as:
    # - '2021', '2022'... OR
    # - 'Total allocation 2021', 'Standard allocation 2021', 'NER allocation 2021'
    year_cols: Dict[int, Dict[str, str]] = {}
    for c in df.columns:
        nc = _norm_col(c)
        m = re.search(r"(20\d{2})", nc)
        if not m:
            continue
        y = int(m.group(1))
        kind = "total"
        if "standard" in nc:
            kind = "standard"
        elif re.search(r"\bner\b", nc) or "new entrant" in nc:
            kind = "ner"
        elif "total" in nc:
            kind = "total"
        year_cols.setdefault(y, {})[kind] = c

    if not year_cols:
        raise ValueError("Could not find any year allocation columns (e.g., 2021..2026).")

    rows = []
    for y, kinds in sorted(year_cols.items()):
        total_col = kinds.get("total") or kinds.get("standard")  # fall back
        if total_col is None:
            continue
        tmp = pd.DataFrame({
            "permit_id": df[permit_col].astype(str).str.strip(),
            "year": y,
            "allocation_total": pd.to_numeric(df[total_col], errors="coerce"),
        })
        if inst_id_col:
            tmp["installation_id"] = df[inst_id_col].astype(str).str.strip()
        if inst_col:
            tmp["installation_name"] = df[inst_col].astype(str).str.strip()
        if op_col:
            tmp["operator_name"] = df[op_col].astype(str).str.strip()

        if "standard" in kinds:
            tmp["allocation_standard"] = pd.to_numeric(df[kinds["standard"]], errors="coerce")
        if "ner" in kinds:
            tmp["allocation_ner"] = pd.to_numeric(df[kinds["ner"]], errors="coerce")
        rows.append(tmp)

    out = pd.concat(rows, ignore_index=True)
    out = out.dropna(subset=["allocation_total"]).reset_index(drop=True)
    return out


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--xlsx", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    args = p.parse_args()

    out = read_allocation_table(args.xlsx)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out, index=False)
    print(f"Wrote {len(out):,} rows -> {args.out}")


if __name__ == "__main__":
    main()
