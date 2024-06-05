#!/bin/bash

sidewinder-build-shards \
  --shard-definition-file src/sidewinder/setup/config/tpch_shard_generation_queries.yaml \
  --shard-manifest-file data/shards/manifests/s3_tpch_sf1_shard_manifest.yaml \
  --shard-count 11 \
  --source-data-path data/tpch/sf=1 \
  --output-data-path s3://voltrondata-sidewinder/shards/tpch/sf=1 \
  --overwrite
