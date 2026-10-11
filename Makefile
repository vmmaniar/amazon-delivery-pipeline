export PYTHONPATH := src
CSV ?= data/raw/amazon_delivery.csv

run:     ## load, clean, check, build star schema and marts
	python3 -m pipeline.run all --csv $(CSV)
sample:  ## same, on the 2,000-row sample
	python3 -m pipeline.run all --csv data/sample/amazon_delivery_sample.csv
report:  ## charts + docs/FINDINGS.md
	python3 -m pipeline.report
