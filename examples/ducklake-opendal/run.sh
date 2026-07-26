#!/usr/bin/env bash
# Runs the DuckLake + duckdb-opendal demo in a clean working directory.
# Downloads a local DuckDB CLI if none (>= 1.5.5) is on PATH.
set -euo pipefail

cd "$(dirname "$0")"

DUCKDB_BIN="${DUCKDB_BIN:-duckdb}"
if ! command -v "$DUCKDB_BIN" >/dev/null 2>&1; then
    if [ ! -x ./duckdb ]; then
        echo "duckdb not found on PATH, downloading the latest CLI..."
        curl -sSL -o duckdb.zip \
            https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-amd64.zip
        unzip -o duckdb.zip duckdb
        rm duckdb.zip
    fi
    DUCKDB_BIN=./duckdb
fi

"$DUCKDB_BIN" --version

# Start from a clean slate so the demo is reproducible.
rm -rf lake_storage metadata.ducklake metadata.ducklake.files
mkdir -p lake_storage

"$DUCKDB_BIN" -c ".read demo.sql"

echo "=== native filesystem view of the OpenDAL root (lake_storage) ==="
find lake_storage -type f | sort

echo "=== proof: fresh DuckDB process re-reads the lake through OpenDAL ==="
"$DUCKDB_BIN" -c "
LOAD ducklake; LOAD opendal;
CREATE SECRET opendal_local (TYPE fs, SCOPE 'fs://', config MAP {'root': 'lake_storage'});
ATTACH 'ducklake:metadata.ducklake' AS lake (DATA_PATH 'fs:///data/');
SELECT count(*) AS metric_rows_from_fresh_process FROM lake.metrics;
"
