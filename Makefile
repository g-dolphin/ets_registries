PYTHON ?= python

EUTL_DIR ?= _raw/euets/eutl_2024_202410
UK_ALLOCATION_XLSX ?= _raw/ukets/ukets-allocation-table-december-2025.xlsx
UK_COMPLIANCE_XLSX ?= _raw/ukets/20250611_Compliance_Report_Emissions_and_Surrenders.xlsx
CA_MRR_DIR ?= _raw/california/emissions
CA_ALLOCATION_SECTOR_CSV ?= _raw/california/allowanceAllocation/nc-allocation_v2023.csv
OUT_FACILITY ?= _output/facility_level.csv
OUT_SECTOR ?= _output/sector_level.csv
OUT_SECTOR_ISIC3 ?= _output/sector_level_isic3.csv

.PHONY: run

run:
	PYTHONPATH=src $(PYTHON) -m registry_processing.pipeline \
		--eutl-dir $(EUTL_DIR) \
		--uk-allocation-xlsx $(UK_ALLOCATION_XLSX) \
		--uk-compliance-xlsx $(UK_COMPLIANCE_XLSX) \
		--ca-mrr-raw-dir $(CA_MRR_DIR) \
		--ca-allocation-sector-csv $(CA_ALLOCATION_SECTOR_CSV) \
		--out-facility $(OUT_FACILITY) \
		--out-sector $(OUT_SECTOR) \
		--out-sector-isic3 $(OUT_SECTOR_ISIC3)
