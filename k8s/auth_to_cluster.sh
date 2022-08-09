#!/bin/bash

aws configure sso
# SSO start URL [None]: https://voltrondata.awsapps.com/start
# SSO Region [None]: us-east-1
# Select voltron-data-demo AWS Account
# Select VICE-Developer
export AWS_PROFILE=VICE-Developer-795371563663
export AWS_REGION=us-east-2
aws eks --region us-east-2 update-kubeconfig --name demo
kubectl config set-context --current --namespace=sw
