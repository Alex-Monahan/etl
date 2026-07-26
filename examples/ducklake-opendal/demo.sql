-- Minimal working example: DuckLake storing its data files through the
-- duckdb-opendal community extension (https://github.com/chitralverma/duckdb-opendal).
--
-- Requires DuckDB >= 1.5.5. Run via ./run.sh or:
--   duckdb -c ".read demo.sql"

INSTALL ducklake;
INSTALL opendal FROM community;
LOAD ducklake;
LOAD opendal;

-- Route the fs:// scheme through OpenDAL, rooted at ./lake_storage.
-- Swap TYPE/config for any other OpenDAL service (s3, memory, ...) to move the
-- lake's data files to that backend without changing anything else below.
CREATE SECRET opendal_local (
    TYPE fs,
    SCOPE 'fs://',
    config MAP {'root': 'lake_storage'}
);

-- DuckLake catalog lives in a local DuckDB file; every table data file is
-- read and written through OpenDAL because DATA_PATH uses the fs:// scheme.
ATTACH 'ducklake:metadata.ducklake' AS lake (DATA_PATH 'fs:///data/');
USE lake;

CREATE TABLE events (id BIGINT, name VARCHAR, ts TIMESTAMP);
INSERT INTO events VALUES
    (1, 'signup', TIMESTAMP '2026-07-26 10:00:00'),
    (2, 'login',  TIMESTAMP '2026-07-26 10:05:00');
UPDATE events SET name = 'first_login' WHERE id = 2;

-- A larger table so DuckLake writes real Parquet files through OpenDAL.
CREATE TABLE metrics AS
    SELECT range AS id, random() AS value FROM range(100_000);

-- Push catalog-inlined rows out to Parquet on the OpenDAL data path.
CALL ducklake_flush_inlined_data('lake');

.print === query results served from DuckLake tables ===
SELECT * FROM events ORDER BY id;
SELECT count(*) AS metric_rows, round(avg(value), 2) AS avg_value FROM metrics;

.print === time travel: events before the UPDATE ===
SELECT * FROM events AT (VERSION => 2) ORDER BY id;

.print === DuckLake snapshot history ===
SELECT snapshot_id, changes FROM lake.snapshots() ORDER BY snapshot_id;

.print === data files as seen THROUGH OpenDAL (opendal_ls) ===
SELECT path, metadata.content_length AS bytes
FROM opendal_ls('fs:///', recursive=true)
WHERE metadata."mode" = 'file'
ORDER BY path;

.print === read DuckLake parquet files back directly via the opendal fs:// scheme ===
SELECT count(*) AS rows_across_parquet_files
FROM read_parquet('fs:///data/**/*.parquet');
