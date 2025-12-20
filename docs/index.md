# ETS Registries

This repository provides a harmonised processing pipeline for multiple emissions trading systems (ETS):

- **EU ETS** (EUTL extracts)
- **UK ETS** (allocation table + public registry compliance report)
- **California Cap-and-Trade** (CARB MRR + sector allocation totals)

The integrated pipeline produces **two outputs**:

1. A **facility-level** dataset (facility-year rows, harmonised schema across systems)
2. A **sector-level** dataset aligned to the **EU ETS NACE aggregation schema**

Allocation is provided for three metrics (where data allow):

- **Observed** (EU/UK)
- **Reconstructed** (California)
- **Counterfactual** (all systems; may be `NaN` if inputs are missing)

