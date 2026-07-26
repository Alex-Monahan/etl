# DuckLake + duckdb-opendal

Minimal working example of DuckDB's [DuckLake](https://ducklake.select) lakehouse
extension storing and reading all of its table data files through the
[duckdb-opendal](https://github.com/chitralverma/duckdb-opendal) community
extension, which exposes [Apache OpenDAL](https://github.com/apache/opendal)
storage services as DuckDB filesystems.

See [CACHING.md](CACHING.md) for the companion design notes on building a
Foyer-based write-through cache (local or as a separate cache service) for
DuckLake.

## Requirements

- DuckDB >= 1.5.5 (the minimum version for the `opendal` community extension).
- Network access to `extensions.duckdb.org` and `community-extensions.duckdb.org`.

## Run it

```bash
./run.sh
```

The script downloads a DuckDB CLI if none is on PATH, wipes the local demo
state, runs `demo.sql`, and then re-attaches the lake from a second, fresh
DuckDB process to prove durability.

## How the two extensions compose

The `opendal` extension registers OpenDAL-backed filesystems in DuckDB's
virtual filesystem under URL schemes such as `fs://`, `s3://`, and
`memory://`, configured through `CREATE SECRET`:

```sql
CREATE SECRET opendal_local (
    TYPE fs,
    SCOPE 'fs://',
    config MAP {'root': 'lake_storage'}
);
```

DuckLake does not know or care which filesystem serves its `DATA_PATH`; it
just issues reads and writes against DuckDB's virtual filesystem. Pointing
`DATA_PATH` at an OpenDAL scheme routes every DuckLake data file through
OpenDAL:

```sql
ATTACH 'ducklake:metadata.ducklake' AS lake (DATA_PATH 'fs:///data/');
```

Because `fs://` is only handled by the opendal extension, every Parquet data
file, delete file, and file listing for the lake goes through OpenDAL's
operator stack. Swapping the secret's `TYPE` and `config` (for example to
`s3`) relocates the lake's storage without touching the DuckLake side.

## What the demo proves

`demo.sql` exercises the full read/write path and verifies it from both sides:

1. `CREATE TABLE` / `INSERT` / `UPDATE` on DuckLake tables succeed with the
   data path on `fs:///data/`, including a 100k-row table that forces real
   Parquet writes and `ducklake_flush_inlined_data` to flush catalog-inlined
   rows to storage.
2. Queries against the tables return correct results, and time travel
   (`AT (VERSION => 2)`) reconstructs the pre-`UPDATE` state from the files.
3. `opendal_ls('fs:///', recursive=true)` lists the Parquet data and delete
   files DuckLake wrote, as seen through OpenDAL itself.
4. `read_parquet('fs:///data/**/*.parquet')` reads the same files back
   directly via the OpenDAL scheme.
5. `run.sh` shows the same files on the native filesystem under the OpenDAL
   root (`lake_storage/`) and re-attaches the lake from a fresh DuckDB process.

Sample output from a real run:

```text
=== DuckLake snapshot history ===
┌─────────────┬───────────────────────────────────────────────────────────┐
│ snapshot_id │                          changes                          │
├─────────────┼───────────────────────────────────────────────────────────┤
│           0 │ {schemas_created=[main]}                                  │
│           1 │ {tables_created=[main.events]}                            │
│           2 │ {inlined_insert=[1]}                                      │
│           3 │ {inlined_insert=[1], inlined_delete=[1]}                  │
│           4 │ {tables_created=[main.metrics], tables_inserted_into=[2]} │
│           5 │ {flushed_inlined=[1]}                                     │
└─────────────┴───────────────────────────────────────────────────────────┘
=== data files as seen THROUGH OpenDAL (opendal_ls) ===
┌───────────────────────────────────────────────────────────────────────────────┬─────────┐
│                                     path                                      │  bytes  │
├───────────────────────────────────────────────────────────────────────────────┼─────────┤
│ data/main/events/ducklake-019fa026-b9e2-785c-b679-acfbad9dd2a8.parquet        │     892 │
│ data/main/events/ducklake-019fa026-b9e6-7c9d-ae6d-e440656138cc-delete.parquet │     893 │
│ data/main/metrics/ducklake-019fa026-b991-7270-9110-8c69a5ac465e.parquet       │ 1201377 │
└───────────────────────────────────────────────────────────────────────────────┴─────────┘
```

## Notes and caveats

- The demo keeps the DuckLake catalog in a local DuckDB file
  (`metadata.ducklake`); only the data path goes through OpenDAL. For a
  multi-client lake, host the catalog in Postgres and keep the same
  OpenDAL-backed `DATA_PATH`.
- The opendal extension can also take over schemes normally served by native
  extensions (`SET opendal_override_native_filesystems = 's3';`), which lets an
  existing `DATA_PATH 's3://...'` lake run through OpenDAL unchanged.
- Small inserts are inlined into the catalog by default and only hit object
  storage when flushed; that is DuckLake behavior, independent of OpenDAL.
