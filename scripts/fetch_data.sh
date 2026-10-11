#!/usr/bin/env bash
# Download the Amazon Delivery dataset (43,739 rows). The original is on Kaggle
# (sujalsuralkar/amazon-delivery-dataset, needs a login); this is a public mirror of the same CSV.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p data/raw
curl -fsSL -o data/raw/amazon_delivery.csv \
  https://raw.githubusercontent.com/bruteforcerxx/Optimizing-Delivery-Logistics/main/amazon_delivery.csv
wc -l data/raw/amazon_delivery.csv
