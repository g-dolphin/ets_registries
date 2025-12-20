# Classification concordance

The pipeline harmonises sector information to **NACE Rev.2** where possible.
For non-European sources (e.g., US NAICS), a **first-pass** concordance is provided.

- NAICS → NACE/ISIC mapping is implemented as a transparent, ETS-focused crosswalk (prefix-based),
  and is **not** intended to be a complete official concordance.
- You can extend or replace the mapping by editing the concordance table and/or the sector mapping
  logic in `registry_processing.harmonize`.

