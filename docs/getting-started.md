# Getting started

## Install (recommended)

Create a virtual environment and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .
```

## Quick run

Run the integrated pipeline:

```bash
python3 -m registry_processing.pipeline \
  --eutl-dir _raw/euets/eutl_2024_202410 \
  --uk-allocation-xlsx _raw/ukets/ukets-allocation-table-december-2025.xlsx \
  --uk-compliance-xlsx _raw/ukets/20250611_Compliance_Report_Emissions_and_Surrenders.xlsx \
  --ca-mrr-raw-dir _raw/california \
  --ca-allocation-sector-csv _raw/california/nc-allocation_v2023.csv \
  --out-facility _output/facility_level.csv \
  --out-sector _output/sector_level.csv
```

If you only want a subset of systems, omit the corresponding inputs.

Alternatively, use the Makefile target:

```bash
make run
```
