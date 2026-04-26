# TIF Council Pipeline

Builds a Chicago TIF dataset for district-by-year and project-by-year analysis, and harvests post-2010 source PDFs.

## Run
From `tasks/tif_council_pipeline/code`:

```bash
make
```

## Easy Sources First (Recommended)
Before long eLMS runs, collect high-signal/low-friction sources first:

```bash
make collect-docs-easy
```

This does two steps:
- Runs `build_tif_pipeline.py` to refresh Socrata datasets and Legistar matter attachments/PDFs.
- Runs `collect_tif_documents.py --skip-elms 1` to collect annual report PDF links/files and dataset counts without eLMS keyword crawling.
- Runs `build_gap_driven_layers.py` to rebuild the cumulative document inventory, `2010-2024` project spine, match-status tables, and targeted document gap queue.

Optional caps:

```bash
make collect-docs-easy EASY_LEG_MAX_MATTERS=8000 EASY_LEG_MAX_PDF=1500 EASY_LEG_MAX_PDF_ATTEMPTS=3000 MIN_REPORT_YEAR=2010 MAX_REPORT_PDF=0
```

If you have a Legistar API token, pass it to improve attachment-file retrieval:

```bash
make collect-docs-easy LEGISTAR_TOKEN=YOUR_TOKEN
```

To refresh the gap-driven inventory and validation layer without re-running downloads:

```bash
make refresh-gap-driven
```

Useful overrides for legacy annual-report extraction:

```bash
make refresh-gap-driven MAX_LEGACY_PDFS=25 OCR_SEARCH_START_PAGE=12 OCR_SEARCH_END_PAGE=18
```

To audit the legacy PDF extraction against direct PDF text/OCR evidence:

```bash
make audit-legacy-pdf-extraction AUDIT_SAMPLE_SIZE=20
```

To build the pre-2010 City Clerk journal legislation evidence queue:

```bash
make journal-legislation-smoke
make journal-legislation JOURNAL_YEAR_START=1981 JOURNAL_YEAR_END=2010
```

The journal task lives in `tasks/tif_journal_legislation`. It reads the locally downloaded City Clerk journal PDFs from the adjacent `alderman_data` project, writes its evidence/review outputs there, and merges selected journal PDFs into `output/tif_document_inventory.csv` with `source = city_clerk_journal`.

Note: `meta_count` in `tif_elms_search_term_counts.csv` is the eLMS search-result count for a keyword query, not a count of distinct TIF projects.

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

One-command resume wrapper (recommended for long runs):

```bash
./run_elms_auto_resume.sh start
```

Wrapper helpers:

```bash
./run_elms_auto_resume.sh status
./run_elms_auto_resume.sh logs
./run_elms_auto_resume.sh stop
```

`start` automatically reads the latest `term_index` + `next_skip` checkpoint from `output/tif_elms_batch_run_log.csv` and resumes from there.
By default the wrapper runs in a resilient mode: `MAX_BATCHES_PER_TERM=500`, `RETRY_FAILURES=8`, `RETRY_PAUSE_SECONDS=5`, and `STOP_ON_ERROR=0`.

Optional override example:

```bash
RETRY_FAILURES=12 RETRY_PAUSE_SECONDS=10 STOP_ON_ERROR=0 ./run_elms_auto_resume.sh start
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
- `output/tif_projects_by_district_year_2010_2024.csv`
- `output/tif_projects_master.csv`
- `output/tif_projects_with_master_match.csv`
- `output/tif_projects_with_geometry.csv`
- `output/tif_project_year_spine.csv`
- `output/tif_project_spine.csv`
- `output/tif_project_match_status.csv`
- `output/tif_document_gap_queue.csv`
- `output/tif_district_universe.csv`
- `output/tif_district_year_universe.csv`
- `output/tif_district_universe_summary.csv`
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
- `output/tif_document_inventory.csv`
- `output/tif_document_inventory_summary.csv`
- `output/tif_matter_inventory.csv`
- `output/tif_attachment_inventory.csv`
- `output/tif_harvest_run_log.csv`
- `output/tif_legacy_annual_report_projects_2010_2016.csv`
- `output/tif_legacy_annual_report_extract_summary.csv`
- `output/tif_legacy_pdf_audit_sample.csv`
- `output/tif_legacy_pdf_project_audit_sample.csv`
- `output/tif_legacy_pdf_audit_summary.csv`
- `output/tif_collection_validation_summary.csv`

Manual exception/config layers live in:
- `config/project_name_overrides.csv`
- `config/matter_link_overrides.csv`
- `config/known_missing_documents.csv`

Raw PDFs are saved under:
- `input/elms/pdf/`
- `input/annual_reports/pdf/`

Notes:
- `tif_elms_matters.csv`, `tif_elms_attachments.csv`, and `tif_elms_pdf_download_status.csv` are append-only cumulative outputs.
- `tif_elms_search_term_counts.csv` and `tif_elms_search_hits.csv` are per-run snapshots.
- Automated batch logs are written to `output/tif_elms_batch_run_log.csv`.
- Wrapper runtime logs are written to `output/elms_auto.log` with PID file `output/elms_auto.pid`.
- `tif_document_inventory.csv` is the cumulative document-level inventory used for validation; it is the preferred source over the latest capped harvest summary.
- `tif_project_year_spine.csv` is the combined `2010-2024` project-year layer with provenance, match status, and geometry source.
- `tif_document_gap_queue.csv` is the targeted queue for follow-up eLMS and document searches.
- `tif_legacy_pdf_audit_sample.csv` compares extracted legacy rows against direct PDF text/OCR evidence for a sampled set of annual-report PDFs.
