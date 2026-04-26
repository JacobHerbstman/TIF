# Pre-2010 TIF Journal Legislation

This task builds an auditable evidence queue for pre-2010 TIF legislation from City Clerk Journals of the Proceedings.

The task starts from the already downloaded journal manifests/PDFs in:

`/Users/jacobherbstman/Desktop/alderman_data/tasks/clerk_journals_download/output`

It does not duplicate journal PDFs. It reads those PDFs in place, writes per-page text artifacts, scans for TIF legislation candidates, and creates review templates for handcoding facts.

## Run

Smoke test the 1990-09-12 gold journal:

```bash
make -C tasks/tif_journal_legislation/code smoke
```

Build the full 1981-2010 queue:

```bash
make -C tasks/tif_journal_legislation/code build
```

Limit a run while tuning:

```bash
make -C tasks/tif_journal_legislation/code build YEAR_START=1990 YEAR_END=1990 MAX_JOURNALS=3
```

## Outputs

All outputs are written under `tasks/tif_journal_legislation/output/`.

- `tif_journal_document_inventory.csv`: selected journal PDFs and source metadata.
- `tif_journal_page_text_index.csv`: one row per PDF page with text method, text hash, section hint, matched terms, and text sidecar path.
- `page_text/`: extracted page text artifacts.
- `tif_legislation_evidence_queue.csv`: one row per candidate event window, with page range, snippet, matched terms, suggested district/project, addresses, dollar amounts, and match suggestions.
- `tif_district_legislation_candidates.csv`: district-level candidate rows extracted from the journal windows, with matched `tif_number` suggestions where the district already exists in the structured universe.
- `tif_district_legislation_rollup_pre2010.csv`: one row per matched or unmatched district key, summarizing likely first journal event, first initial event, and first/largest funding candidates.
- `tif_legislation_facts_review.csv`: handcoding template. Existing reviewed rows are preserved on rerun.
- `tif_district_timeline_pre2010.csv`: confirmed district timeline facts derived only from reviewed rows.
- `tif_project_deal_timeline_pre2010.csv`: confirmed project/deal timeline facts derived only from reviewed rows.
- `tif_journal_legislation_summary.csv`: row counts and candidate counts by priority/event/year.
- `tif_journal_legislation_smoke_test.csv`: smoke check results when using the `smoke` target.

The script also merges selected journal PDF rows into `tasks/tif_council_pipeline/output/tif_document_inventory.csv` with `source = city_clerk_journal`.

## Review Rules

The evidence queue is intentionally broad. Only rows marked `review_status = confirmed`, `accepted`, or `reviewed_confirmed` in `tif_legislation_facts_review.csv` enter the derived timeline outputs.

Use the source PDF, meeting date, page range, and snippet to handcode:

- district/project names,
- counterparty/developer,
- raw and normalized addresses,
- public/TIF funding amounts,
- private funding amounts,
- total project cost,
- timeline date,
- ordinance reference date,
- whether the event is initial or revised.

Unresolved legal descriptions or parcel-only references should stay in raw fields until they can be normalized.
