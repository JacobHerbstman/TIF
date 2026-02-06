# TIF Council Pipeline

Builds a Chicago TIF dataset for district-by-year and project-by-year analysis, and harvests post-2010 source PDFs.

## Run
From `tasks/tif_council_pipeline/code`:

```bash
make
```

## Batch eLMS Runs (Recommended)
Run short, resumable eLMS batches instead of one long crawl:

```bash
make collect-docs-batch TERM_INDEX=0 START_SKIP=0 MAX_MATTERS=120 MAX_DETAIL_CALLS=80 MAX_ELMS_PDF=80
```

Then increment `START_SKIP` for the same `TERM_INDEX`, for example `0`, `120`, `240`, etc.

Automated looping across terms/skips:

```bash
make collect-docs-auto TERM_START=0 TERM_END=4 START_SKIP=0 MAX_BATCHES_PER_TERM=50 MAX_MATTERS=120 MAX_DETAIL_CALLS=80 MAX_ELMS_PDF=80
```

Useful variables:
- `TERM_INDEX`: search-term index in `collect_tif_documents.py` (`-1` means all terms)
- `START_SKIP`: `/matter` pagination offset
- `MAX_MATTERS`: max unique matters to process in that run
- `MAX_DETAIL_CALLS`: cap on `/matter/{id}` detail calls
- `MAX_ELMS_PDF`: cap on PDF downloads
- `RESUME_FROM_CSV`: CSV used to skip already-detailed matter IDs (defaults to `../output/tif_elms_matters.csv`)
- `SKIP_ANNUAL_REPORTS`: set `1` for batch loops to avoid re-crawling annual report pages every run

## Main outputs
- `output/source_inventory.csv`
- `output/tif_coverage_summary.csv`
- `output/tif_projects_by_district_year.csv`
- `output/tif_projects_master.csv`
- `output/tif_projects_with_master_match.csv`
- `output/tif_projects_with_geometry.csv`
- `output/tif_district_year_boundaries.csv`
- `output/tif_district_year_panel.csv`
- `output/tif_district_year_panel_full_1998_2024.csv`
- `output/tif_projects_with_alderman.csv`
- `output/tif_district_year_with_lead_alderman.csv`

## PDF/document harvest outputs
- `output/tif_external_dataset_counts.csv`
- `output/tif_elms_search_term_counts.csv`
- `output/tif_elms_search_hits.csv`
- `output/tif_elms_matters.csv`
- `output/tif_elms_attachments.csv`
- `output/tif_elms_pdf_download_status.csv`
- `output/tif_annual_report_pages.csv`
- `output/tif_annual_report_pdf_links.csv`
- `output/tif_annual_report_pdf_download_status.csv`
- `output/tif_document_harvest_summary.csv`

Raw PDFs are saved under:
- `input/elms/pdf/`
- `input/annual_reports/pdf/`

Notes:
- `tif_elms_matters.csv`, `tif_elms_attachments.csv`, and `tif_elms_pdf_download_status.csv` are append-only cumulative outputs.
- `tif_elms_search_term_counts.csv` and `tif_elms_search_hits.csv` are per-run snapshots.
- Automated batch logs are written to `output/tif_elms_batch_run_log.csv`.
