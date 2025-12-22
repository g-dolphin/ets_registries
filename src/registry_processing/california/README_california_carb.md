# California (CARB) – facility-level free allocation reconstruction

CARB does not publish a facility-by-facility free allocation registry like the EU ETS.
This folder contains a **reproducible workaround** that:

1. ingests **facility-level emissions** from CARB Mandatory Reporting Regulation (MRR)
2. ingests **sector-level allocation totals** published by CARB
3. reconstructs an **estimated** facility-level free allocation using *Option 3* (hybrid intensity-adjusted)

---

## Data you need (public) + where to download

### 1) Facility emissions (MRR)
CARB publishes annual spreadsheets “**GHG Facility and Entity Emissions**” under its Mandatory GHG Reporting program.

Download any year from the **Reported Data** page and save it locally (XLSX).

### 2) Sector-level allowance allocation totals
CARB publishes sector-aggregated allocation summaries and “download chart data” CSVs through its Cap-and-Trade program data pages / dashboards.

Examples (small CSVs that work well as constraints):
- `nc-allocation_v2023.csv` (sector totals by vintage)
- additional vintage summary PDFs are linked from the Cap-and-Trade Program Data page

### 3) Benchmarks and assistance factors (for “Option 3”)
Official **product benchmarks** live in **Table 9-1** of the Cap-and-Trade Regulation; leakage-risk / **assistance factors** are referenced from **Table 8-1**.

Because output is not public for most facilities, this pipeline uses **sector proxy benchmarks** by default.
You provide those proxies using `data_templates/ca_sector_benchmarks_template.csv`.

> If you *do* have facility output (or intensity) from another source, you can supply it and the hybrid method will use it.

---

## What the code does

`carb_free_allocation.py` provides:
- parsers for MRR facility emissions XLSX
- parsers for CARB sector allocation totals CSV
- a reconstruction function implementing **Option 3**:

\[
A_{i,t} = A^{total}_{s,t} \times \frac{E_{i,t}\,(B_s/I_{i,t})^{\alpha}}{\sum_{j\in s} E_{j,t}\,(B_s/I_{j,t})^{\alpha}}
\]

Where:
- `E` = facility covered emissions
- `A_total` = CARB-published sector total allocation
- `B` = benchmark intensity proxy (sector)
- `I` = observed facility intensity (optional; defaults to `B`)
- `alpha` = tuning parameter (0–1)

---

## Quickstart (CLI)

```bash
python3 -m registry_processing.california.carb_free_allocation \
  --mrr_xlsx /path/to/2024-ghg-emissions-2025-11-04.xlsx \
  --allocation_csv /path/to/nc-allocation_v2023.csv \
  --benchmarks_csv data_templates/ca_sector_benchmarks_template.csv \
  --alpha 0.5 \
  --out_csv out/ca_free_allocation_estimated_2024.csv
```

Notes:
- If you have facility intensity, pass a CSV with columns `arb_id,year,intensity` and add `--facility_intensity_csv ...`.
- If you don’t, the method reduces to within-sector proportional-to-emissions.

---

## Assumptions / caveats

- Sector mapping from MRR “Industry Sector” → CARB allocation sectors is heuristic; adjust the mapping dict in code for your use case.
- Sector totals sometimes refer to **vintage** while emissions refer to **report year**; be explicit in your pipeline about which crosswalk you use.

