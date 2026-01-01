"""PDF parsing for Washington Ecology no-cost allowances.

As of the first compliance period, Ecology publishes *aggregated* EITE allocations (>=5 facilities per row).
We parse the 'Summary of Allowance Allocations to EITEs for <year>' table from PDFs.

The parser is intentionally tolerant:
  * handles line-wrapped subsector names
  * accepts commas or no commas in numeric columns
  * returns an empty dataframe if it cannot find a table

Dependencies: `pdfplumber` is optional; we fall back to `pypdf` text extraction.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Iterable, Optional

import pandas as pd


def _extract_text(path: Path) -> str:
    path = Path(path)
    # Prefer pdfplumber when installed (better layout), otherwise fall back to pypdf
    try:
        import pdfplumber  # type: ignore
        with pdfplumber.open(str(path)) as pdf:
            return "\n".join((page.extract_text() or "") for page in pdf.pages)
    except Exception:
        try:
            from pypdf import PdfReader  # type: ignore
            reader = PdfReader(str(path))
            return "\n".join((page.extract_text() or "") for page in reader.pages)
        except Exception as e:
            raise RuntimeError(f"Failed to extract text from PDF: {path}") from e


_NUM = re.compile(r"\b\d[\d,]*\b")


def parse_eite_allocation_table(pdf_path: Path) -> pd.DataFrame:
    """Parse the EITE allocation summary table from a single PDF.

    Returns columns:
      * allocation_year (int, inferred from 'for 2023' etc.)
      * subsector (str)
      * total_facilities (int)
      * total_allowances (int)
      * source_pdf (str)
    """
    pdf_path = Path(pdf_path)
    txt = _extract_text(pdf_path)

    # infer year: look for 'Summary of Allowance Allocations to EITEs for 2023'
    m_year = re.search(r"Summary\s+of\s+Allowance\s+Allocations\s+to\s+EITEs\s+for\s+(\d{4})", txt, flags=re.IGNORECASE)
    allocation_year: Optional[int] = int(m_year.group(1)) if m_year else None

    # locate the header of the table
    header_idx = None
    lines = [re.sub(r"\s+", " ", ln).strip() for ln in txt.splitlines()]
    for i, ln in enumerate(lines):
        if re.search(r"Subsector\s+Total\s+Facilities\s+Total\s+Allowances", ln, flags=re.IGNORECASE):
            header_idx = i
            break
    if header_idx is None:
        return pd.DataFrame(columns=["allocation_year", "subsector", "total_facilities", "total_allowances", "source_pdf"])

    rows = []
    # after header, we expect 5 subsectors + Total row, but we parse until a blank or 'Pub No.' etc.
    buffer = ""
    for ln in lines[header_idx + 1 :]:
        if not ln:
            continue
        if re.search(r"^Pub\s+No\.|^List\s+of\s+NAICS|^\d\s+RCW\b", ln, flags=re.IGNORECASE):
            break
        if re.search(r"^Total\b", ln, flags=re.IGNORECASE):
            # stop before total row (we can keep it if desired)
            break

        # accumulate subsector names that wrapped across lines
        nums = _NUM.findall(ln)
        if len(nums) >= 2:
            # this line has the numeric columns -> finish a row
            facilities = int(nums[-2].replace(",", ""))
            allowances = int(nums[-1].replace(",", ""))
            name_part = ln
            # remove the numeric tail
            name_part = re.sub(r"\s+" + re.escape(nums[-2]) + r"\s+" + re.escape(nums[-1]) + r"\s*$", "", name_part).strip()
            subsector = (buffer + " " + name_part).strip() if buffer else name_part.strip()
            buffer = ""
            if subsector:
                rows.append({
                    "allocation_year": allocation_year,
                    "subsector": subsector,
                    "total_facilities": facilities,
                    "total_allowances": allowances,
                    "source_pdf": pdf_path.name,
                })
        else:
            # likely a wrapped name line
            buffer = (buffer + " " + ln).strip() if buffer else ln.strip()

    return pd.DataFrame(rows)


def parse_allocation_dir(pdf_dir: Path) -> pd.DataFrame:
    """Parse all PDFs in a directory and concatenate results."""
    pdf_dir = Path(pdf_dir)
    dfs = []
    for pdf in sorted(pdf_dir.glob("*.pdf")):
        df = parse_eite_allocation_table(pdf)
        if len(df):
            dfs.append(df)
    if not dfs:
        return pd.DataFrame(columns=["allocation_year", "subsector", "total_facilities", "total_allowances", "source_pdf"])
    out = pd.concat(dfs, ignore_index=True)
    # drop duplicates (in case same PDF saved twice)
    out = out.drop_duplicates(subset=["allocation_year", "subsector", "total_allowances"])
    return out
