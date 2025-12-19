# EU ETS (euets.info / EUTL processed data)

This folder contains utilities to aggregate installation-level compliance data to sector level.

## Usage

Assuming your repository layout is:

- `ets_registries_data/_raw/eutl_2024_202410/` (contains `compliance.csv`, `installation.csv`, `nace_code.csv`)
- `ets_registries_data/code/euets/aggregate_sector_nace.py`

Run:

```bash
python code/euets/aggregate_sector_nace.py \
  --eutl-dir _raw/eutl_2024_202410 \
  --out _output/euets/euets_sector_nace_year.csv \
  --system euets
```

Output columns include:
- `year`, `country_id`, `nace_code`, `nace_description`, `isic4_code`
- `verified` (verified emissions, tCO2)
- `allocatedFree` (free allocation, allowances)
- `free_share = allocatedFree / verified`
