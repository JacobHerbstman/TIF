.PHONY: all link-inputs

all: link-inputs
	python3 extract_projected_realized_from_pdfs.py \
		--pdf-dir ../input/pdfs \
		--attachments-csv ../input/tif_attachments.csv \
		--matters-csv ../input/tif_matters.csv \
		--projects-master-csv ../input/tif_projects_master.csv \
		--projects-annual-csv ../input/tif_projects_by_district_year.csv \
		--output-mentions ../output/tif_pdf_projected_realized_mentions.csv \
		--output-pairs ../output/tif_pdf_projected_realized_pairs.csv \
		--output-summary ../output/tif_pdf_projected_realized_summary.csv

link-inputs:
	mkdir -p ../input ../output
	ln -sfn ../../tif_council_pipeline/input/elms/pdf ../input/pdfs
	ln -sfn ../../tif_council_pipeline/output/tif_elms_attachments.csv ../input/tif_attachments.csv
	ln -sfn ../../tif_council_pipeline/output/tif_elms_matters.csv ../input/tif_matters.csv
	ln -sfn ../../tif_council_pipeline/output/tif_projects_master.csv ../input/tif_projects_master.csv
	ln -sfn ../../tif_council_pipeline/output/tif_projects_by_district_year.csv ../input/tif_projects_by_district_year.csv
