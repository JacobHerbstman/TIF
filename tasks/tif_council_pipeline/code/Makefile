.PHONY: all link-inputs collect-docs collect-docs-batch collect-docs-auto collect-docs-auto-resume collect-docs-easy refresh-gap-driven audit-legacy-pdf-extraction journal-legislation journal-legislation-smoke

all: link-inputs
	python3 build_tif_pipeline.py --input-dir ../input --output-dir ../output --max-matters 1500 --max-pdf 250
	$(MAKE) collect-docs

collect-docs:
	python3 collect_tif_documents.py --input-dir ../input --output-dir ../output --min-report-year 2010
	$(MAKE) refresh-gap-driven

collect-docs-easy:
	python3 build_tif_pipeline.py \
		--input-dir ../input \
		--output-dir ../output \
		--max-matters $(or $(EASY_LEG_MAX_MATTERS),8000) \
		--max-pdf $(or $(EASY_LEG_MAX_PDF),1200) \
		--max-pdf-attempts $(or $(EASY_LEG_MAX_PDF_ATTEMPTS),2400) $(if $(LEGISTAR_TOKEN),--legistar-token $(LEGISTAR_TOKEN),)
	python3 collect_tif_documents.py \
		--input-dir ../input \
		--output-dir ../output \
		--skip-elms 1 \
		--min-report-year $(or $(MIN_REPORT_YEAR),2010) \
		--max-report-pdf $(or $(MAX_REPORT_PDF),0)
	$(MAKE) refresh-gap-driven

refresh-gap-driven:
	python3 build_gap_driven_layers.py \
		--input-dir ../input \
		--output-dir ../output \
		--config-dir ../config \
		--legacy-start-year $(or $(LEGACY_START_YEAR),2010) \
		--legacy-end-year $(or $(LEGACY_END_YEAR),2016) \
		--max-legacy-pdfs $(or $(MAX_LEGACY_PDFS),0) \
		--ocr-search-start-page $(or $(OCR_SEARCH_START_PAGE),12) \
		--ocr-search-end-page $(or $(OCR_SEARCH_END_PAGE),18)

audit-legacy-pdf-extraction:
	python3 audit_legacy_pdf_extraction.py \
		--output-dir ../output \
		--sample-size $(or $(AUDIT_SAMPLE_SIZE),20)

journal-legislation:
	$(MAKE) -C ../../tif_journal_legislation/code build \
		PIPELINE_OUTPUT_DIR=../../tif_council_pipeline/output \
		YEAR_START=$(or $(JOURNAL_YEAR_START),1981) \
		YEAR_END=$(or $(JOURNAL_YEAR_END),2010) \
		MAX_JOURNALS=$(or $(JOURNAL_MAX_JOURNALS),0)

journal-legislation-smoke:
	$(MAKE) -C ../../tif_journal_legislation/code smoke \
		PIPELINE_OUTPUT_DIR=../../tif_council_pipeline/output

# Example:
# make collect-docs-batch TERM_INDEX=0 START_SKIP=0 MAX_MATTERS=120 MAX_DETAIL_CALLS=80 MAX_ELMS_PDF=80
collect-docs-batch:
	python3 collect_tif_documents.py \
		--input-dir ../input \
		--output-dir ../output \
		--min-report-year 2010 \
		--term-index $(or $(TERM_INDEX),-1) \
		--start-skip $(or $(START_SKIP),0) \
		--max-matters $(or $(MAX_MATTERS),200) \
		--max-detail-calls $(or $(MAX_DETAIL_CALLS),100) \
		--max-elms-pdf $(or $(MAX_ELMS_PDF),100) \
		--resume-from-csv $(or $(RESUME_FROM_CSV),../output/tif_elms_matters.csv)

collect-docs-auto:
	python3 run_elms_batches.py \
		--input-dir ../input \
		--output-dir ../output \
		--start-term-index $(or $(TERM_START),0) \
		--end-term-index $(or $(TERM_END),4) \
		--start-skip $(or $(START_SKIP),0) \
		--max-batches-per-term $(or $(MAX_BATCHES_PER_TERM),50) \
		--max-matters-per-run $(or $(MAX_MATTERS),200) \
		--max-detail-calls $(or $(MAX_DETAIL_CALLS),100) \
		--max-elms-pdf $(or $(MAX_ELMS_PDF),100) \
		--resume-from-csv $(or $(RESUME_FROM_CSV),../output/tif_elms_matters.csv) \
		--skip-annual-reports $(or $(SKIP_ANNUAL_REPORTS),1) \
		--retry-failures $(or $(RETRY_FAILURES),2) \
		--retry-pause-seconds $(or $(RETRY_PAUSE_SECONDS),2) \
		--stop-on-error $(or $(STOP_ON_ERROR),1) \
		--sleep-seconds $(or $(SLEEP_SECONDS),0)

collect-docs-auto-resume:
	./run_elms_auto_resume.sh start

link-inputs:
	mkdir -p ../input ../output
	ln -sfn ../../../data_raw ../input/data_raw
