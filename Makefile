PY ?= python3
export PYTHONPATH := src
CSV ?= data/raw/amazon_delivery.csv

.PHONY: up data run report test sample
up:            ## start Postgres
	docker compose up -d postgres
data:          ## download the full dataset
	./scripts/fetch_data.sh
run:           ## load + clean + DQ + star schema + marts
	$(PY) -m pipeline.run all --csv $(CSV)
sample:        ## run on the committed 2,000-row sample
	$(PY) -m pipeline.run all --csv data/sample/amazon_delivery_sample.csv
report:        ## charts + docs/FINDINGS.md from the loaded warehouse
	$(PY) -m pipeline.report
test:
	$(PY) -m pytest -q
