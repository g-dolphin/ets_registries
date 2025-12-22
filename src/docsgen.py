"""Utilities to generate documentation pages from the live codebase.

This module is used by CI to keep certain documentation sections (schemas, CLI)
synchronised with code changes.
"""

from __future__ import annotations

from pathlib import Path
from typing import List
import sys
import pandas as pd

# Ensure repo_root/code is on PYTHONPATH so `import registry_processing` works
CODE_DIR = Path(__file__).resolve().parent  # .../ets_registries/code
if str(CODE_DIR) not in sys.path:
    sys.path.insert(0, str(CODE_DIR))

from registry_processing.pipeline import FACILITY_SCHEMA
from registry_processing.harmonize import SECTOR_COLS_EU_SCHEMA


def _md_table(headers: List[str], rows: List[List[str]]) -> str:
    out = []
    out.append("| " + " | ".join(headers) + " |")
    out.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for r in rows:
        out.append("| " + " | ".join(r) + " |")
    return "\n".join(out) + "\n"


def write_schemas_markdown(out_path: Path) -> None:
    """Write the schemas page used by MkDocs.

    Args:
        out_path: Destination markdown file (e.g., docs/_generated/schemas.md)
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)

    facility_rows = [[c] for c in FACILITY_SCHEMA]
    sector_rows = [[c] for c in SECTOR_COLS_EU_SCHEMA]

    md = []
    md.append("# Schemas\n")
    md.append("This page is **auto-generated** from the live codebase.\n")
    md.append("## Facility-level schema\n")
    md.append(_md_table(["column"], facility_rows))
    md.append("## Sector-level schema (EU schema)\n")
    md.append(_md_table(["column"], sector_rows))

    out_path.write_text("\n".join(md), encoding="utf-8")


def generate_all(repo_root: Path) -> None:
    """Generate all docs that are derived from code.

    Args:
        repo_root: Repository root directory (contains the `docs/` folder).
    """
    write_schemas_markdown(repo_root / "docs" / "_generated" / "schemas.md")
