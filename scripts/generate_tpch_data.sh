#!/bin/bash

set -e

SCRIPT_DIR=$(dirname ${0})

TPCH_SCALE_FACTOR=${1:?You MUST provide the TPC-H Scale Factor!}

echo "TPCH_SCALE_FACTOR=${TPCH_SCALE_FACTOR}"

DATA_DIR="${SCRIPT_DIR}/../data/tpch/${TPCH_SCALE_FACTOR}"
mkdir -p "${DATA_DIR}"

duckdb << EOF
.bail on
.echo on
SELECT VERSION();
CALL dbgen(sf=${TPCH_SCALE_FACTOR});
EXPORT DATABASE '${DATA_DIR}' (FORMAT PARQUET);
EOF

# Rename the parquet files to have number (to match the pattern)
cd ${DATA_DIR}
for file in customer.parquet lineitem.parquet orders.parquet part.parquet partsupp.parquet supplier.parquet
do
  file_name=$(echo "${file}" | cut -d "." -f 1)
  mkdir ${file_name}
  mv "${file}" "${file_name}/${file_name}.1.parquet"
done

for file in nation.parquet region.parquet
do
  file_name=$(echo "${file}" | cut -d "." -f 1)
  mkdir ${file_name}
  mv "${file}" "${file_name}/${file_name}.parquet"
done

echo -e "All done."
