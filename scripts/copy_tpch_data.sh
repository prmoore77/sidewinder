#!/bin/bash
# This script assumes you've installed the Google Cloud CLI and that you have authenticated (with aws configure sso or init)

set -e

TPCH_SCALE_FACTOR=${1:?You MUST provide the TPC-H Scale Factor!}

echo "TPCH_SCALE_FACTOR=${TPCH_SCALE_FACTOR}"

DATA_DIR="/home/app_user/data/tpch_${TPCH_SCALE_FACTOR}"
mkdir -p "${DATA_DIR}"
aws s3 cp s3://voltrondata-tpch/${TPCH_SCALE_FACTOR}/parquet/ "${DATA_DIR}" --recursive
