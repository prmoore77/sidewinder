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

set -e

SCRIPT_DIR=$(dirname ${0})

SSB_SCALE_FACTOR=${1:?You MUST provide the TPC-H Scale Factor!}

echo "SSB_SCALE_FACTOR=${SSB_SCALE_FACTOR}"

DATA_DIR="${SCRIPT_DIR}/../data/ssb/sf=${SSB_SCALE_FACTOR}"
mkdir -p "${DATA_DIR}"

# Parallelize the copy of data (helps for Scale Factors greater than 1)...
for i in {0..9};
do
  nohup aws s3 cp s3://voltrondata-demo-data/ssb/sf=${SSB_SCALE_FACTOR}/ "${DATA_DIR}" \
    --exclude="*" --include="*.*${i}.parquet" --recursive | tee -a copy.log &
done

wait

echo -e "All done."
