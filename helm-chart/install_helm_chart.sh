#!/bin/bash

kubectl config set-context --current --namespace=sw

helm upgrade sidewinder --install .
