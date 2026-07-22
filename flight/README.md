# ETL → DuckLake proof of concept on MotherDuck Flights

This directory contains a proof of concept that runs the `etl-replicator`
binary (Supabase ETL, Postgres logical replication via `pgoutput`) inside a
MotherDuck flight.

## What the flight does

1. **Environment recon** — prints arch, distro, glibc, CPU, disk.
2. **Bootstrap** — installs `git`, `curl`, and `postgresql` with `apt-get`.
3. **Binary provisioning** — clones this fork branch with `git`, and:
   - uses the cached compiled binary from `flight-cache/` when it is present
     and loadable on the flight runtime, otherwise
   - installs rustup + the pinned toolchain and compiles
     `etl-replicator` (`--no-default-features --features ducklake`) in the
     flight, then pushes the fresh cache back to this branch when a GitHub
     token secret is configured.
4. **Source database** — starts local Postgres with `wal_level=logical`,
   creates `sourcedb` with an `orders` table and a publication, plus a
   `ducklake_catalog` database that serves as the DuckLake catalog.
5. **Object storage** — starts MinIO on `127.0.0.1:9100` (port 9000 is taken
   by the replicator's Prometheus exporter) and creates the `etl-poc` bucket.
6. **Replication** — seeds 1,000 rows (exercises the initial COPY phase),
   starts `etl-replicator`, then runs periodic rounds of inserts, updates,
   and deletes against the source table.
7. **Verification** — attaches the DuckLake catalog from Python DuckDB and
   polls until `count`, `sum(id)`, and `sum(amount)` in
   `lake.public.orders` match the live source table.

## Files

- `main.py` — the flight source (single-file entrypoint, see
  MotherDuck Flights docs).
- `../flight-cache/etl-replicator-x86_64-unknown-linux-gnu.gz` — compiled
  release binary cache pulled by the flight.
- `../flight-cache/manifest.json` — provenance for the cached binary
  (source commit, target triple, glibc it was built against).

## Replicator configuration used

The flight writes `APP_CONFIG_DIR` config files at runtime; the effective
`prod.yaml` shape is:

```yaml
destination:
  ducklake:
    catalog_url: "postgres://postgres:<pw>@127.0.0.1:5432/ducklake_catalog"
    data_path: "s3://etl-poc/lake"
    s3_access_key_id: "<minio key>"
    s3_secret_access_key: "<minio secret>"
    s3_region: "us-east-1"
    s3_endpoint: "127.0.0.1:9100"
    s3_url_style: "path"
    s3_use_ssl: false
pipeline:
  id: 1
  publication_name: "etl_poc_pub"
  pg_connection:
    host: "127.0.0.1"
    port: 5432
    name: "sourcedb"
    username: "postgres"
    password: "<pw>"
    tls:
      trusted_root_certs: ""
      enabled: false
  batch:
    max_fill_ms: 1000
```

Notes discovered while building this:

- The replicator binary requires an `s3://` (or `gs://`) `data_path`; the
  `file://` scheme is only accepted by the library API. Hence MinIO.
- Config loading requires both `base.yaml` and `{environment}.yaml` to exist
  (`APP_ENVIRONMENT` defaults to `prod`).
- The Prometheus exporter binds `[::]:9000` unconditionally.
- With no vendored DuckDB extension directory present, the destination falls
  back to online `INSTALL ducklake; INSTALL postgres; INSTALL httpfs;`
  (bundled DuckDB 1.5.3), so the flight needs outbound network access.
- `run_source_migrations` defaults to `true` and installs a
  `ddl_command_end` event trigger, which requires superuser on the source —
  fine here because the flight owns its Postgres.
