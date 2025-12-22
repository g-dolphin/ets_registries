"""Option 3 (hybrid intensity-adjusted) allocation reconstruction.

Use this when you have:
- installation-level emissions (E_i)
- (optional) installation-level intensity proxy (I_i) or output
- sector totals A_s to distribute across installations

Weights:
    w_i = E_i * (B_s / I_i)^alpha

Where:
- B_s is a sector benchmark (proxy benchmark is fine)
- I_i is an observed intensity proxy; if missing, assumed equal to B_s
- alpha in [0,1] controls strength of intensity adjustment

Then:
    A_i = A_s * w_i / sum_{j in s} w_j

This module is intentionally generic; sector mapping is handled upstream.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd


@dataclass
class Option3Config:
    alpha: float = 0.5
    eps: float = 1e-12


def option3_allocate(
    df: pd.DataFrame,
    *,
    sector_col: str,
    emissions_col: str,
    sector_total_col: str,
    benchmark_col: Optional[str] = None,
    intensity_col: Optional[str] = None,
    cfg: Option3Config = Option3Config(),
    out_col: str = "allocation_option3",
) -> pd.DataFrame:
    out = df.copy()

    E = pd.to_numeric(out[emissions_col], errors="coerce").fillna(0.0).astype(float)
    A_s = pd.to_numeric(out[sector_total_col], errors="coerce").fillna(0.0).astype(float)

    if benchmark_col and benchmark_col in out.columns:
        B = pd.to_numeric(out[benchmark_col], errors="coerce").astype(float)
    else:
        # If no benchmark provided, set B=1 so ratio depends only on intensity (or equals 1)
        B = pd.Series(1.0, index=out.index)

    if intensity_col and intensity_col in out.columns:
        I = pd.to_numeric(out[intensity_col], errors="coerce").astype(float)
    else:
        I = pd.Series(np.nan, index=out.index, dtype=float)

    # If I missing, assume I=B (ratio=1)
    I = I.where(np.isfinite(I), B)

    ratio = (B / (I + cfg.eps)).clip(lower=cfg.eps)
    w = E * (ratio ** cfg.alpha)

    out["_w"] = w
    out["_A_s"] = A_s

    # group normalize
    denom = out.groupby(sector_col)["_w"].transform("sum").replace(0.0, np.nan)
    out[out_col] = out["_A_s"] * out["_w"] / denom
    out[out_col] = out[out_col].fillna(0.0)

    out = out.drop(columns=["_w", "_A_s"])
    return out
