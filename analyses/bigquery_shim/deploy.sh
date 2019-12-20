#!/bin/bash
set -x

cd $(dirname "$0")

python3 setup.py bdist_egg
egg=$(find dist | sort | tail -n1)

# https://docs.databricks.com/dev-tools/databricks-cli.html#databricks-cli
dbfs cp "${egg}" dbfs:/eggs/