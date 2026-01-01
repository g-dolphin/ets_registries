"""Washington (WA) Climate Commitment Act — Cap-and-Invest registry ingestion.

This subpackage provides *source-specific* ingestion for:
  * Facility/Reporter GHG emissions (WA GHG Reporting Program public publication export)
  * No-cost allowances PDFs (sector-aggregated EITE totals)

The integrated ETS registries pipeline should call `washington.ingest_facility.read_wa_facility_year`.
"""

from .ingest_facility import read_wa_facility_year

__all__ = ["read_wa_facility_year"]
