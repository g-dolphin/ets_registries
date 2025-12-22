"""Download public UK ETS data files.

We intentionally require the caller to pass URLs (or keep defaults) so that
the exact provenance is explicit in your registry run logs.

Example:
python3 -m registry_processing.ukets.download_public_files \
  --outdir data/raw/ukets \
  --allocation-url "<GOV.UK direct xlsx url>" \
  --compliance-url "<registry reports direct xlsx url>"
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
import urllib.request


def download(url: str, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(url) as resp, open(out_path, "wb") as f:
        f.write(resp.read())


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--outdir", type=Path, required=True)
    p.add_argument("--allocation-url", type=str, required=True,
                  help="Direct URL to the GOV.UK allocation table .xlsx")
    p.add_argument("--compliance-url", type=str, required=True,
                  help="Direct URL to the UK ETS registry compliance report .xlsx")
    p.add_argument("--allocation-filename", type=str, default="ukets_allocation_table.xlsx")
    p.add_argument("--compliance-filename", type=str, default="ukets_compliance_emissions_surrenders.xlsx")
    args = p.parse_args()

    alloc_path = args.outdir / args.allocation_filename
    comp_path = args.outdir / args.compliance_filename

    print(f"Downloading allocation table -> {alloc_path}")
    download(args.allocation_url, alloc_path)

    print(f"Downloading compliance report -> {comp_path}")
    download(args.compliance_url, comp_path)

    print("Done.")


if __name__ == "__main__":
    main()
