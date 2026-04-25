.PHONY: all build smoke derived-clean

OUTPUT_DIR ?= ../output
PIPELINE_OUTPUT_DIR ?= ../../tif_council_pipeline/output
JOURNAL_PDF_ROOT ?= /Users/jacobherbstman/Desktop/alderman_data/tasks/clerk_journals_download/output/journals
MANIFEST_1980_1998 ?= /Users/jacobherbstman/Desktop/alderman_data/tasks/clerk_journals_download/output/journal_manifest_1980_1998.csv
MANIFEST_1999_2010 ?= /Users/jacobherbstman/Desktop/alderman_data/tasks/clerk_journals_download/output/journal_manifest_1999_2010.csv
YEAR_START ?= 1981
YEAR_END ?= 2010
MAX_JOURNALS ?= 0
OCR_EMPTY_PAGES ?= 0

all: build

build:
	python3 build_tif_journal_legislation.py \
		--manifest-csv "$(MANIFEST_1980_1998)" \
		--manifest-csv "$(MANIFEST_1999_2010)" \
		--journal-pdf-root "$(JOURNAL_PDF_ROOT)" \
		--output-dir "$(OUTPUT_DIR)" \
		--pipeline-output-dir "$(PIPELINE_OUTPUT_DIR)" \
		--year-start "$(YEAR_START)" \
		--year-end "$(YEAR_END)" \
		--max-journals "$(MAX_JOURNALS)" \
		--ocr-empty-pages "$(OCR_EMPTY_PAGES)"

smoke:
	python3 build_tif_journal_legislation.py \
		--manifest-csv "$(MANIFEST_1980_1998)" \
		--manifest-csv "$(MANIFEST_1999_2010)" \
		--journal-pdf-root "$(JOURNAL_PDF_ROOT)" \
		--output-dir "$(OUTPUT_DIR)" \
		--pipeline-output-dir "$(PIPELINE_OUTPUT_DIR)" \
		--year-start 1990 \
		--year-end 1990 \
		--meeting-date 1990-09-12 \
		--smoke-test 1
