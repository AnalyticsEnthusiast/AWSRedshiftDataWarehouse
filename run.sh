#!/bin/bash

# Execution wrapper script

PYTHON_EXE=$(which python)


# 1. Create redshift DWH
${PYTHON_EXE} create_warehouse.py
RC=$(echo $?)
[ "${RC}" = "0" ] && echo "Warehouse successfully created" || echo "Warehouse creation failed"

# 2. Run create table statements
${PYTHON_EXE} create_tables.py
RC=$(echo $?)
[ "${RC}" = "0" ] && echo "Tables successfully created" || echo "Table creation failed"

# 3. Run etl pipeline to load data into staging, then into destination tables
${PYTHON_EXE} etl.py
RC=$(echo $?)
[ "${RC}" = "0" ] && echo "ETL successfully run" || echo "ETL Failed"


