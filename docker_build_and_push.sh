#!/bin/bash

set -e

# Be sure to login first..
# aws configure sso
# SSO start URL [None]: https://voltrondata.awsapps.com/start
# SSO Region [None]: us-east-1

aws ecr get-login-password --region us-east-2 | docker login --username AWS --password-stdin 795371563663.dkr.ecr.us-east-2.amazonaws.com

docker buildx build --tag 795371563663.dkr.ecr.us-east-2.amazonaws.com/sw:latest --platform linux/amd64,linux/arm64 --push .
