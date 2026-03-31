#!/bin/bash
# Exact comparison of goosedb output vs DuckDB output.
# Usage: ./check_correctness.sh result.csv duckdb_result.csv

if [ $# -ne 2 ]; then
    echo "Usage: $0 <goosedb_result.csv> <duckdb_result.csv>"
    exit 1
fi

goosedb_val=$(tail -1 "$1" | tr -d '\r')
duckdb_val=$(tail -1 "$2" | tr -d '\r')

if [ "$goosedb_val" = "$duckdb_val" ]; then
    echo "PASS: $goosedb_val"
else
    echo "FAIL: goosedb=$goosedb_val  duckdb=$duckdb_val"
    exit 1
fi
