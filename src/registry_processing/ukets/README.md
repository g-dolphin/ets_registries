# UK ETS processing (public data)

This module ingests **public UK ETS** datasets and produces a harmonized
installation-by-year panel suitable for downstream calculations.

Public sources used:
- GOV.UK: UK ETS allocation table for operators of installations (2021–2026)
- UK ETS Registry public reports: Compliance report (Emissions & Surrenders)

Scripts
- `download_public_files.py`: download latest public files (pass URLs explicitly for reproducibility).
- `parse_allocation_table.py`: parse allocation table (.xlsx) to tidy long CSV.
- `parse_compliance_report.py`: parse registry compliance report (.xlsx) to tidy long CSV.
- `pipeline_ukets.py`: end-to-end runner that merges allocations + emissions and can run Option 3 reconstruction.
- `option3_reconstruct.py`: Option 3 hybrid allocation function (for counterfactual allocation reconstructions).

Notes
- UK ETS publishes *installation-level* free allocation. In most use-cases you should treat that as ground truth.
- Option 3 is included for harmonization/counterfactuals (e.g., if you only have sector totals and need to allocate to installations).

