"""Plot EU ETS free_share by ISIC 3-digit sectors.

Outputs:
1) EU-level time series (2005–2024) in a 3x2 grid for selected ISIC categories
2) 2024 cross-section across selected EU countries in a 3x2 grid

Usage:
  PYTHONPATH=src python3 -m plotting.plot_isic3_free_share \
    --input _output/sector_level_isic3.csv \
    --out-dir _output
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


ISIC_CATEGORIES = [
    ("351", "Power"),
    ("239", "Cement"),
    ("170", "Paper"),
    ("202", "Chemicals"),
    ("891", "Minerals"),
]

EU_COUNTRIES = [
    ("FR", "France"),
    ("DE", "Germany"),
    ("GR", "Greece"),
    ("HU", "Hungary"),
    ("IT", "Italy"),
    ("PL", "Poland"),
    ("PT", "Portugal"),
    ("RO", "Romania"),
    ("ES", "Spain"),
    ("SE", "Sweden"),
]

EU27 = [
    "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE",
    "GR", "HU", "IE", "IT", "LV", "LT", "LU", "MT", "NL", "PL", "PT",
    "RO", "SK", "SI", "ES", "SE",
]


def _digits3(x) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip()
    if s == "":
        return ""
    try:
        f = float(s)
        if f.is_integer():
            s = str(int(f))
        else:
            s = str(f).replace(".", "")
    except ValueError:
        pass
    digits = "".join(ch for ch in s if ch.isdigit())
    if len(digits) >= 3:
        return digits[:3]
    return digits.ljust(3, "0")


def load_data(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["isic3_code"] = df["isic3_code"].apply(_digits3)
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["free_share"] = pd.to_numeric(df["free_share"], errors="coerce")
    df["allocatedFree"] = pd.to_numeric(df["allocatedFree"], errors="coerce")
    df["verified"] = pd.to_numeric(df["verified"], errors="coerce")
    # Backfill free_share when possible
    missing = df["free_share"].isna()
    can_compute = missing & df["allocatedFree"].notna() & df["verified"].notna() & (df["verified"] > 0)
    df.loc[can_compute, "free_share"] = df.loc[can_compute, "allocatedFree"] / df.loc[can_compute, "verified"]
    return df


def eu_aggregate(df: pd.DataFrame, countries: list[str]) -> pd.DataFrame:
    sub = df[df["country_id"].isin(countries)].copy()
    grp = (
        sub.groupby(["year", "isic3_code"], dropna=False)
        .agg(
            allocatedFree=("allocatedFree", lambda x: x.sum(min_count=1)),
            verified=("verified", lambda x: x.sum(min_count=1)),
        )
        .reset_index()
    )
    grp["free_share"] = grp["allocatedFree"] / grp["verified"]
    return grp


def plot_timeseries(
    df: pd.DataFrame,
    outpath: Path,
    categories: list[tuple[str, str]],
    nrows: int = 3,
    ncols: int = 2,
) -> None:
    fig, axes = plt.subplots(nrows=nrows, ncols=ncols, sharex=True, sharey=True, figsize=(12, 9))
    axes = np.array(axes).reshape(-1)

    for i, (code, label) in enumerate(categories):
        ax = axes[i]
        sub = df[df["isic3_code"] == code].copy()
        sub = sub[(sub["year"] >= 2005) & (sub["year"] <= 2024)]
        sub = sub.dropna(subset=["year", "free_share"])
        sub = sub.sort_values("year")

        ax.plot(sub["year"].astype(int), sub["free_share"], linewidth=2.0)
        ax.set_title(f"{code} — {label}", fontsize=11)
        ax.axhline(0, linewidth=0.8, alpha=0.4)
        ax.grid(True, axis="y", linewidth=0.6, alpha=0.35)
        if i % ncols == 0:
            ax.set_ylabel("Free allocation share")
        if i >= (nrows - 1) * ncols:
            ax.set_xlabel("Year")

    for j in range(len(categories), len(axes)):
        axes[j].axis("off")

    fig.suptitle("EU ETS — Free allocation share by ISIC (EU aggregate), 2005–2024", fontsize=14, y=0.98)
    fig.tight_layout(rect=[0, 0, 1, 0.96])

    outpath.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(outpath, dpi=220)


def plot_cross_section(
    df: pd.DataFrame,
    outpath: Path,
    categories: list[tuple[str, str]],
    countries: list[tuple[str, str]],
    year: int = 2023,
    nrows: int = 3,
    ncols: int = 2,
) -> None:
    fig, axes = plt.subplots(nrows=nrows, ncols=ncols, sharex=False, sharey=True, figsize=(12, 9))
    axes = np.array(axes).reshape(-1)

    country_order = [c for c, _ in countries]
    country_labels = [n for _, n in countries]

    for i, (code, label) in enumerate(categories):
        ax = axes[i]
        sub = df[(df["isic3_code"] == code) & (df["year"] == year)].copy()
        sub = sub.set_index("country_id").reindex(country_order)

        ax.bar(country_labels, sub["free_share"], color="#4477AA")
        ax.set_title(f"{code} — {label}", fontsize=11)
        ax.axhline(0, linewidth=0.8, alpha=0.4)
        ax.grid(True, axis="y", linewidth=0.6, alpha=0.35)
        ax.tick_params(axis="x", rotation=40, labelsize=8)
        if i % ncols == 0:
            ax.set_ylabel("Free allocation share")

    for j in range(len(categories), len(axes)):
        axes[j].axis("off")

    fig.suptitle(f"EU ETS — Free allocation share by ISIC (selected countries), {year}", fontsize=14, y=0.98)
    fig.tight_layout(rect=[0, 0, 1, 0.96])

    outpath.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(outpath, dpi=220)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--input", type=Path, required=True)
    p.add_argument("--out-dir", type=Path, default=Path("_output/plots"))
    p.add_argument("--metric", type=str, default="observed", help="allocation_metric to filter (default: observed)")
    args = p.parse_args()

    df = load_data(args.input)
    df = df[(df["reportedInSystem_id"] == "euets") & (df["allocation_metric"] == args.metric)].copy()

    # EU aggregate for time series (EU27)
    df_eu = eu_aggregate(df, EU27)

    # Country-level for cross-section
    df_countries = df[df["country_id"].isin([c for c, _ in EU_COUNTRIES])].copy()

    out_ts = args.out_dir / "free_share_eu_isic3_timeseries.png"
    out_xs = args.out_dir / "free_share_eu_isic3_2024_countries.png"

    plot_timeseries(df_eu, out_ts, ISIC_CATEGORIES)
    plot_cross_section(df_countries, out_xs, ISIC_CATEGORIES, EU_COUNTRIES, year=2023)

    print(f"Saved: {out_ts}")
    print(f"Saved: {out_xs}")


if __name__ == "__main__":
    main()
