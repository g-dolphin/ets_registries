# Washington Cap-and-Invest (Climate Commitment Act) ingestion

## Inputs (expected on disk)

* Emissions / reporting export (CSV), manually downloaded from WA open data:
  `.../_raw/washington/emissions/*.csv`
* Allowance allocation PDFs, manually downloaded from Ecology "No-cost allowances" page:
  `.../_raw/washington/allowances/*.pdf`

## Outputs

`read_wa_facility_year(...)` returns a harmonised facility-year dataframe compatible with the
higher-level ETS registries pipeline.

Important: WA Ecology does **not** publish facility-level free allocation totals. The PDFs provide
**subsector-aggregated** totals for EITE industries. We therefore:

* keep `allocation_observed_free` as `NaN`
* compute `allocation_reconstructed_free` as a **proportional-to-emissions** allocation within each
  PDF subsector and year (best-effort, purely for modelling / reconciliation; not official)
* keep `allocation_counterfactual_free` equal to the proportional reconstruction (no intensity inputs)

If/when Ecology publishes facility-level allocations, this module can be upgraded to populate
`allocation_observed_free`.

## Quick usage

```python
from pathlib import Path
from washington import read_wa_facility_year

df = read_wa_facility_year(
    emissions_csv=Path("/Users/geoffroydolphin/GitHub/ets_registries/_raw/washington/emissions/GHG_Reporting_Program_Publication_YYYYMMDD.csv"),
    allowances_pdf_dir=Path("/Users/geoffroydolphin/GitHub/ets_registries/_raw/washington/allowances"),
)
```
