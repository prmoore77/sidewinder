#!/bin/bash
# This script assumes you've installed the Amazon Cloud CLI and that you have authenticated to Amazon Cloud.
# After authenticating to Amazon - select account: "voltrondata-developers"
# Then set your Environment variables for use with the cli
# - see: https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html?icmpid=docs_sso_user_portal
#
# Example:
#export AWS_ACCESS_KEY_ID="FOO"
#export AWS_SECRET_ACCESS_KEY="BAR"
#export AWS_SESSION_TOKEN="ZEE"

SCRIPT_DIR=$(dirname ${0})

set -e

TPCH_SCALE_FACTOR=${1:?You MUST provide the TPC-H Scale Factor!}

echo "TPCH_SCALE_FACTOR=${TPCH_SCALE_FACTOR}"

DATA_DIR="${SCRIPT_DIR}/../data/tpch_${TPCH_SCALE_FACTOR}"
mkdir -p "${DATA_DIR}"
aws s3 sync s3://voltrondata-tpch/${TPCH_SCALE_FACTOR}/parquet/ "${DATA_DIR}" --recursive
