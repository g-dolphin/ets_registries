# Validation flags

The pipeline is **non-interrupting**: validation checks do not raise errors; they write boolean flags to the facility output.

Current facility-level flags include:

- `flag_allocation_observed_free_negative`
- `flag_allocation_reconstructed_free_negative`
- `flag_allocation_counterfactual_free_negative`
- `flag_surrender_lt_emissions`

Interpretation:

- Allocation flags: the corresponding allocation column is < 0
- Surrender flag: `allowances_surrendered < emissions_verified` (when both are available)

