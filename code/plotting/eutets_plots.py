# Plot EU ETS free_share (allocatedFree / verified) over 2005–2024
# - One subplot per NACE activity (top 10 by total verified emissions)
# - Multiple selected countries shown as separate lines in each subplot
#
# Expected input: euets_sector_nace_year.csv with (at least) columns like:
#   year, country, nace_id (or nace_code), verified, allocatedFree, free_share
# Optional: nace_description
#
# Usage:
#   python plot_free_share.py /path/to/euets_sector_nace_year.csv

from __future__ import annotations

import sys
import math
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def _find_col(df: pd.DataFrame, candidates: list[str], required: bool = True) -> str | None:
    cols_lower = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols_lower:
            return cols_lower[cand.lower()]
    if required:
        raise KeyError(f"Could not find any of these columns: {candidates}. Available: {list(df.columns)}")
    return None


def load_data(path: str | Path) -> pd.DataFrame:
    df = pd.read_csv(path)

    # column detection
    c_year = _find_col(df, ["year", "compliance_year"])
    c_country = _find_col(df, ["country", "jurisdiction", "iso2", "iso3"])
    c_nace = _find_col(df, ["nace_id", "nace_code", "nace"])
    c_desc = _find_col(df, ["nace_description", "nace_desc", "activity_description"], required=False)

    c_verified = _find_col(df, ["verified", "verified_emissions", "emissions", "verified_sum"], required=False)
    c_free = _find_col(df, ["allocatedFree", "free_allocation", "allocated_free", "allocatedfree_sum"], required=False)
    c_free_share = _find_col(df, ["free_share", "freeShare", "free_allocation_share"], required=False)

    # standardize
    out = pd.DataFrame({
        "year": pd.to_numeric(df[c_year], errors="coerce").astype("Int64"),
        "country": df[c_country].astype(str),
        "nace": df[c_nace].astype(str),
    })
    if c_desc is not None:
        out["nace_desc"] = df[c_desc].astype(str)
    else:
        out["nace_desc"] = ""

    if c_verified is not None:
        out["verified"] = pd.to_numeric(df[c_verified], errors="coerce")
    else:
        out["verified"] = np.nan

    if c_free is not None:
        out["allocatedFree"] = pd.to_numeric(df[c_free], errors="coerce")
    else:
        out["allocatedFree"] = np.nan

    if c_free_share is not None:
        out["free_share"] = pd.to_numeric(df[c_free_share], errors="coerce")
    else:
        out["free_share"] = np.nan

    # compute free_share if missing
    need = out["free_share"].isna()
    can_compute = need & out["verified"].notna() & out["allocatedFree"].notna() & (out["verified"] > 0)
    out.loc[can_compute, "free_share"] = out.loc[can_compute, "allocatedFree"] / out.loc[can_compute, "verified"]

    # filter years & plausible values
    out = out[(out["year"].between(2005, 2024))].copy()
    out["free_share"] = out["free_share"].clip(lower=0, upper=2)  # allow some noise >1; clip for plotting

    # fill description per NACE if partially missing
    if "nace_desc" in out.columns and out["nace_desc"].ne("").any():
        desc_map = (out.loc[out["nace_desc"].ne(""), ["nace", "nace_desc"]]
                      .drop_duplicates()
                      .groupby("nace")["nace_desc"]
                      .first())
        out["nace_desc"] = out["nace"].map(desc_map).fillna(out["nace_desc"]).replace({"": np.nan})
    else:
        out["nace_desc"] = np.nan

    return out


def select_top_nace(df: pd.DataFrame, top_n: int = 10) -> list[str]:
    # "Most important" = highest total verified emissions across 2005–2024 (all countries)
    if df["verified"].notna().any():
        totals = (df.groupby("nace", dropna=False)["verified"]
                    .sum(min_count=1)
                    .sort_values(ascending=False))
    else:
        # fallback: use allocatedFree if verified missing (unlikely)
        totals = (df.groupby("nace", dropna=False)["allocatedFree"]
                    .sum(min_count=1)
                    .sort_values(ascending=False))
    return totals.head(top_n).index.tolist()


def select_countries(df: pd.DataFrame, countries: list[str] | None = None, top_k: int = 6) -> list[str]:
    if countries is not None and len(countries) > 0:
        return countries

    # default: top emitting countries overall (2005–2024)
    if df["verified"].notna().any():
        totals = (df.groupby("country")["verified"].sum(min_count=1).sort_values(ascending=False))
    else:
        totals = (df.groupby("country")["allocatedFree"].sum(min_count=1).sort_values(ascending=False))
    return totals.head(top_k).index.tolist()


def plot_free_share_facets(
    df: pd.DataFrame,
    top_nace: list[str],
    countries: list[str],
    rolling_years: int = 1,
    ncols: int = 2,
    figsize_per_panel: tuple[float, float] = (7.2, 3.2),
    outpath: str | Path | None = None,
) -> None:
    d = df[df["nace"].isin(top_nace) & df["country"].isin(countries)].copy()

    # optional smoothing
    if rolling_years and rolling_years > 1:
        d = d.sort_values(["nace", "country", "year"])
        d["free_share_plot"] = (
            d.groupby(["nace", "country"], dropna=False)["free_share"]
             .transform(lambda s: s.rolling(rolling_years, min_periods=max(1, rolling_years // 2)).mean())
        )
    else:
        d["free_share_plot"] = d["free_share"]

    # layout
    n = len(top_nace)
    nrows = math.ceil(n / ncols)
    fig_w = figsize_per_panel[0] * ncols
    fig_h = figsize_per_panel[1] * nrows

    fig, axes = plt.subplots(nrows=nrows, ncols=ncols, sharex=True, sharey=True, figsize=(fig_w, fig_h))
    axes = np.array(axes).reshape(-1)

    # consistent style (matplotlib default colors)
    for i, nace in enumerate(top_nace):
        ax = axes[i]
        sub = d[d["nace"] == nace]

        # title with description if available
        desc = (df.loc[df["nace"] == nace, "nace_desc"].dropna().astype(str).unique().tolist())
        title = f"NACE {nace}"
        if len(desc) > 0:
            title = f"{title} — {desc[0]}"
        ax.set_title(title, fontsize=11)

        for c in countries:
            s = sub[sub["country"] == c].dropna(subset=["year", "free_share_plot"])
            if s.empty:
                continue
            ax.plot(s["year"].astype(int), s["free_share_plot"], linewidth=1.8, alpha=0.95, label=c)

        ax.axhline(0, linewidth=0.8, alpha=0.4)
        ax.set_ylim(0, 1.05)  # free_share is typically in [0,1]
        ax.grid(True, axis="y", linewidth=0.6, alpha=0.35)

        if i % ncols == 0:
            ax.set_ylabel("Free allocation share\n(allocatedFree / verified)")
        if i >= (nrows - 1) * ncols:
            ax.set_xlabel("Year")

    # hide unused panels
    for j in range(n, len(axes)):
        axes[j].axis("off")

    # one legend for the whole figure
    handles, labels = axes[0].get_legend_handles_labels()
    if handles:
        fig.legend(handles, labels, loc="lower center", ncol=min(len(labels), 6), frameon=False)

    fig.suptitle("EU ETS — Free allocation share by activity (top 10 NACE) and selected countries, 2005–2024",
                 fontsize=14, y=0.995)

    fig.tight_layout(rect=[0, 0.06, 1, 0.97])

    if outpath is not None:
        outpath = Path(outpath)
        outpath.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(outpath, dpi=220)
        print(f"Saved: {outpath}")

    plt.show()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python plot_free_share.py path/to/euets_sector_nace_year.csv")

    csv_path = Path(sys.argv[1])

    df = load_data(csv_path)
    top_nace = select_top_nace(df, top_n=10)

    # Option A: auto-select top emitting countries overall
    countries = select_countries(df, countries=None, top_k=6)

    # Option B: manually select countries (uncomment and edit)
    # countries = ["DE", "FR", "IT", "ES", "PL", "NL"]

    plot_free_share_facets(
        df=df,
        top_nace=top_nace,
        countries=countries,
        rolling_years=1,   # set to 3 for a 3-year rolling mean if desired
        ncols=2,
        outpath="free_share_facets.png",
    )
