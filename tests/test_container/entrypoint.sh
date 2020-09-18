#!/bin/bash

# Dockerfile Entrypoint Script

USAGE="$0 run"

OPT=$1;shift
VIRTUAL_ENV=""

setup_env() {
  pushd tests/test_container || exit 1
  pipenv sync
  VIRTUAL_ENV=$(pipenv --venv)
  popd || exit 1
}

run_tests() {
  source ${VIRTUAL_ENV}/bin/activate
  pip install pytest pytest-mock
  python -m pytest
}

if [ "$OPT" = "run" ]; then
  setup_env
  run_tests
else
  echo "ERROR: $USAGE" 1>&2;
  exit 2
fi