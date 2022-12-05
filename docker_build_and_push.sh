#!/bin/bash

set -e

docker login
docker buildx build --tag prmoorevoltron/sidewinder:latest --platform linux/amd64,linux/arm64 --push .
