"""EU ETS sector-year ingestion in the canonical EU schema.

We keep this separate from generic aggregation so that the integrated pipeline
can preserve EU-specific columns that exist in EUTL compliance data:
  * allocatedNewEntrance
  * allocated10c
  * allocatedTotal
  * balance
  * penalty

For other systems, these columns are typically unavailable and are left NaN.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from .aggregate_sector_nace import load_inputs, aggregate


def read_euets_sector_nace_year(eutl_dir: Path, system: str = "euets") -> pd.DataFrame:
    comp, inst, nace = load_inputs(Path(eutl_dir))
    out = aggregate(comp, inst, nace)
    if system != "all":
        out = out[out["reportedInSystem_id"] == system].copy()
    return out
