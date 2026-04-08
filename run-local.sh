#!/bin/bash

set -eo pipefail

export OPERATOR_DOMAIN=poolboy.dev.local
export OPERATOR_NAMESPACE=poolboy-dev
export OPERATOR_MODE=all-in-one
export MANAGE_CLAIMS_INTERVAL=5
export MANAGE_HANDLES_INTERVAL=5
export MANAGE_POOLS_INTERVAL=5

helm template helm --include-crds \
--set deploy=false \
--set admin.deploy=false \
--set nameOverride=poolboy-dev \
--set operatorDomain.name=poolboy.dev.local \
| oc apply -f -

oc create namespace poolboy-dev || :
oc project poolboy-dev

if [[ -d venv ]]; then
  . ./venv/bin/activate
else
  python -m venv ./venv
  . ./venv/bin/activate
  pip install git+https://github.com/rhpds/kopf.git@add-label-selector
  pip install -r dev-requirements.txt
  pip install -r requirements.txt
fi

cd ./operator

exec kopf run \
  --standalone \
  --all-namespaces \
  --liveness=http://0.0.0.0:8080/healthz \
  operator.py
