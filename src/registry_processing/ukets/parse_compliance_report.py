"""Parse UK ETS registry public compliance report (Emissions & Surrenders).

The registry report format can vary:
- sometimes one row per installation with columns like 'Recorded emissions 2021', ...
- sometimes already long with explicit 'Year' column.

We normalize to:
- permit_id
- installation_name (if present)
- year
- recorded_emissions
- allowances_surrendered (if present)
- compliance_status (if present)
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import List, Optional

import pandas as pd


def _norm_col(c: str) -> str:
    return re.sub(r"\s+", " ", str(c)).strip().lower()


def _find_col(df: pd.DataFrame, patterns: List[str]) -> Optional[str]:
    cols = list(df.columns)
    norm = {_norm_col(c): c for c in cols}
    for pat in patterns:
        r = re.compile(pat)
        for nc, orig in norm.items():
            if r.search(nc):
                return orig
    return None


def read_compliance_report(path: Path) -> pd.DataFrame:
    # Public UK ETS registry reports often contain metadata sheets and a "Data" sheet.
    # The 20250611 compliance report uses the "Data" sheet.
    xl = pd.ExcelFile(path)
    sheet = "Data" if "Data" in xl.sheet_names else xl.sheet_names[0]
    df = pd.read_excel(path, sheet_name=sheet)

    permit_col = _find_col(df, [r"^permit id$", r"permit\s*id", r"monitoring plan id"])
    if permit_col is None:
        raise ValueError("Could not find Permit ID column in compliance report.")

    year_col = _find_col(df, [r"^year$", r"reporting year"])
    inst_col = _find_col(df, [r"installation name", r"^installation$"])
    # The compliance report uses year-specific "Static surrender status YYYY" columns.
    status_col = _find_col(df, [r"compliance status", r"surrender status", r"status$"])
    surrendered_col = _find_col(df, [r"allowances surrendered", r"cumulative surrenders", r"surrendered"])
    emissions_col = _find_col(df, [r"recorded emissions", r"verified emissions", r"emissions"])

    operator_col = _find_col(df, [r"account holder name", r"operator name"])
    op_id_col = _find_col(df, [r"operator id$"])
    nace_col = _find_col(df, [r"^nace code$", r"nace"])
    nace_desc_col = _find_col(df, [r"nace description"])

    # If explicit year + emissions columns exist, keep long
    if year_col and emissions_col:
        out = pd.DataFrame({
            "permit_id": df[permit_col].astype(str).str.strip(),
            "year": pd.to_numeric(df[year_col], errors="coerce").astype("Int64"),
            "recorded_emissions": pd.to_numeric(df[emissions_col], errors="coerce"),
        })
        if inst_col:
            out["installation_name"] = df[inst_col].astype(str).str.strip()
        if surrendered_col:
            out["allowances_surrendered"] = pd.to_numeric(df[surrendered_col], errors="coerce")
        if status_col:
            out["compliance_status"] = df[status_col].astype(str).str.strip()
        out = out.dropna(subset=["year", "recorded_emissions"]).reset_index(drop=True)
        out["year"] = out["year"].astype(int)
        return out

    # Otherwise, detect year-specific columns
    year_map = {}
    for c in df.columns:
        nc = _norm_col(c)
        m = re.search(r"(20\d{2})", nc)
        if not m:
            continue
        y = int(m.group(1))
        if "emission" in nc:
            year_map.setdefault(y, {})["recorded_emissions"] = c
        # Some reports provide only cumulative surrenders; keep year-specific surrender columns if present.
        if "surrender" in nc and "status" not in nc and "cumulative" not in nc:
            year_map.setdefault(y, {})["allowances_surrendered"] = c
        if "status" in nc and "surrender" in nc:
            year_map.setdefault(y, {})["compliance_status"] = c

    if not year_map:
        raise ValueError("Could not infer year columns for emissions/surrenders.")

    rows = []
    for y, cols in sorted(year_map.items()):
        if "recorded_emissions" not in cols:
            continue
        tmp = pd.DataFrame({
            "permit_id": df[permit_col].astype(str).str.strip(),
            "year": y,
            "recorded_emissions": pd.to_numeric(df[cols["recorded_emissions"]], errors="coerce"),
        })
        if inst_col:
            tmp["installation_name"] = df[inst_col].astype(str).str.strip()
        if operator_col:
            tmp["operator_name"] = df[operator_col].astype(str).str.strip()
        if op_id_col:
            tmp["operator_id"] = df[op_id_col].astype(str).str.strip()
        if nace_col:
            tmp["nace_code"] = df[nace_col].astype(str).str.strip()
        if nace_desc_col:
            tmp["nace_description"] = df[nace_desc_col].astype(str).str.strip()
        if "allowances_surrendered" in cols:
            tmp["allowances_surrendered"] = pd.to_numeric(df[cols["allowances_surrendered"]], errors="coerce")
        if "compliance_status" in cols:
            tmp["compliance_status"] = df[cols["compliance_status"]].astype(str).str.strip()
        elif status_col:
            # Fall back to a single status column if present.
            tmp["compliance_status"] = df[status_col].astype(str).str.strip()
        rows.append(tmp)

    out = pd.concat(rows, ignore_index=True)
    out = out.dropna(subset=["recorded_emissions"]).reset_index(drop=True)
    return out


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--xlsx", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    args = p.parse_args()

    out = read_compliance_report(args.xlsx)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out, index=False)
    print(f"Wrote {len(out):,} rows -> {args.out}")


if __name__ == "__main__":
    main()
