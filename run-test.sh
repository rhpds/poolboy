#!/bin/bash

set -eo pipefail

. ./venv/bin/activate

cd test

for UNITTEST in unittest-*.py; do
  ./${UNITTEST}
done

ansible-playbook \
-e ansible_python_interpreter='{{ ansible_playbook_python }}' \
-e poolboy_domain=poolboy.dev.local \
-e poolboy_namespace=poolboy-dev \
-e poolboy_service_account=poolboy-dev \
-e poolboy_test_namespace=poolboy-dev-test \
-e '{"poolboy_tests": ["simple"]}' \
playbook.yaml
