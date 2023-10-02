#!/bin/bash

# This script assumes you have duckdb (with all extensions) installed and it is on your system PATH
# You should run ./copy_tpch_data.sh first if you do not have the source parquet data on the local filesystem...

set -e

SCRIPT_DIR=$(dirname ${0})

TPCH_SCALE_FACTOR=${1:?You MUST provide the TPC-H Scale Factor!}
VIEW_OR_TABLE_OPTION=${2:-"VIEW"}

echo "TPCH_SCALE_FACTOR=${TPCH_SCALE_FACTOR}"
echo "VIEW_OR_TABLE_OPTION=${VIEW_OR_TABLE_OPTION}"

DATA_DIR="data/tpch/sf=${TPCH_SCALE_FACTOR}"
DATABASE_FILE="data/tpch_sf${TPCH_SCALE_FACTOR}.duckdb"

pushd "${SCRIPT_DIR}/../"
echo -e "(Re)creating database file: ${DATABASE_FILE}"

rm -f "${DATABASE_FILE}"

duckdb "${DATABASE_FILE}" << EOF
.bail on
.echo on
SELECT VERSION();
CREATE OR REPLACE ${VIEW_OR_TABLE_OPTION} lineitem AS SELECT * FROM read_parquet('${DATA_DIR}/lineitem/*.parquet');
CREATE OR REPLACE ${VIEW_OR_TABLE_OPTION} customer AS SELECT * FROM read_parquet('${DATA_DIR}/customer/*.parquet');
CREATE OR REPLACE ${VIEW_OR_TABLE_OPTION} nation AS SELECT * FROM read_parquet('${DATA_DIR}/nation/*.parquet');
CREATE OR REPLACE ${VIEW_OR_TABLE_OPTION} orders AS SELECT * FROM read_parquet('${DATA_DIR}/orders/*.parquet');
CREATE OR REPLACE ${VIEW_OR_TABLE_OPTION} part AS SELECT * FROM read_parquet('${DATA_DIR}/part/*.parquet');
CREATE OR REPLACE ${VIEW_OR_TABLE_OPTION} partsupp AS SELECT * FROM read_parquet('${DATA_DIR}/partsupp/*.parquet');
CREATE OR REPLACE ${VIEW_OR_TABLE_OPTION} region AS SELECT * FROM read_parquet('${DATA_DIR}/region/*.parquet');
CREATE OR REPLACE ${VIEW_OR_TABLE_OPTION} supplier AS SELECT * FROM read_parquet('${DATA_DIR}/supplier/*.parquet');
EOF

popd

echo "All done."
