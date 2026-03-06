#!/bin/bash

export OPERATOR_DOMAIN=poolboy.dev.local
export OPERATOR_NAMESPACE=poolboy-dev
export OPERATOR_MODE=all-in-one
export MANAGE_CLAIMS_INTERVAL=5
export MANAGE_HANDLES_INTERVAL=5
export MANAGE_POOLS_INTERVAL=5

set -eo pipefail

cd ./operator

exec kopf run \
  --standalone \
  --all-namespaces \
  --liveness=http://0.0.0.0:8080/healthz \
  operator.py
