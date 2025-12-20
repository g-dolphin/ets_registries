# Pipeline

The top-level entrypoint is `registry_processing.pipeline`.

## Inputs

- EU ETS: extracted EUTL dataset directory (expects `compliance.csv`, `installation.csv`, `nace_code.csv`)
- UK ETS:
  - Allocation table (Excel)
  - Compliance report “Emissions and Surrenders” (Excel)
- California:
  - CARB MRR annual spreadsheets (**directory with one file per year**) or a single annual file
  - Sector allocation totals CSV (from CARB Cap-and-Trade data dashboard files)

## Outputs

- Facility-level CSV: one row per facility-year per system
- Sector-level CSV: one row per year × country/region × NACE code per system and allocation metric

## Allocation metrics

- `observed`: taken directly from the system’s published allocation (EU/UK)
- `reconstructed`: facility-level allocation reconstructed from sector totals (California)
- `counterfactual`: Option 3 hybrid within-sector reallocation (all systems where feasible)

