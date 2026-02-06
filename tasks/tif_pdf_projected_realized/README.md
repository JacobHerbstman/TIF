# TIF PDF Projected vs Realized Extractor

Extracts mention-level and paired projected-vs-realized cost/time metrics from TIF-related PDFs.

## Run
From `tasks/tif_pdf_projected_realized/code`:

```bash
make
```

## Inputs
- `input/pdfs` (symlinked from `tasks/tif_council_pipeline/input/elms/pdf`)
- `input/tif_attachments.csv` (symlinked from `output/tif_elms_attachments.csv`)
- `input/tif_matters.csv` (symlinked from `output/tif_elms_matters.csv`)
- `input/tif_projects_master.csv`
- `input/tif_projects_by_district_year.csv`

## Outputs
- `output/tif_pdf_projected_realized_mentions.csv`
- `output/tif_pdf_projected_realized_pairs.csv`
- `output/tif_pdf_projected_realized_summary.csv`

## Notes
- The extractor supports both legacy Legistar-style filenames and current eLMS-style filenames.
- If `input/pdfs` is empty, the pipeline still writes valid empty outputs and a summary.
- Parser uses `pypdf` for text extraction.
