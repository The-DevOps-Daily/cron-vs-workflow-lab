#!/bin/sh
# The orchestrator images bake in a copy of etl/etl.py.
cd "$(dirname "$0")/.." && cp etl/etl.py setups/4-kestra/ && cp etl/etl.py setups/5-prefect/
