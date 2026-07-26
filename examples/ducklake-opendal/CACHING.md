# A Foyer write-through cache for DuckLake

Design notes answering two questions:

1. How can [foyer](https://github.com/foyer-rs/foyer) act as a write-through
   cache for DuckLake?
2. Can foyer power a cache on a separate server (a cache service) for DuckLake?

Companion to the working DuckLake + duckdb-opendal demo in this directory.

## Background: the pieces

- **Apache OpenDAL** (`opendal` crate) is a unified storage access layer: one
  `Operator` API over 50+ services (S3, GCS, Azure, local fs, ...), with
  composable middleware **layers** (retry, metrics, timeout, caching, ...)
  stacked via `.layer(...)`.
- **Foyer** (`foyer` crate) is an embedded hybrid cache for Rust, inspired by
  Meta's CacheLib: a memory tier (`Cache`) plus a disk tier, combined as
  `HybridCache<K, V>` built with `HybridCacheBuilder`. It offers
  request-deduplicated read-through via `get_or_fetch`, pluggable eviction,
  and Prometheus/OTel metrics. Users include RisingWave (S3 block cache),
  Chroma, and SlateDB.
- **The bridge already exists**: since opendal 0.57 (RFC-6370, merged
  2026-01), `opendal::layers::FoyerLayer` is a first-party caching layer,
  enabled with the `layers-foyer` feature and authored by foyer's maintainer.
  Its semantics are exactly a write-through cache:
  - **read**: check the foyer `HybridCache` first; on miss, read from the
    backing service and populate the cache.
  - **write**: write to the backing service, then populate the cache on
    success (write-through, not write-back — durability always comes from the
    backing store).
  - **delete**: invalidate the cache entry.
  - `with_size_limit(...)` caps the object size admitted to the cache.

## Why DuckLake is an unusually good fit for this

DuckLake never mutates or appends to existing data files: Parquet data files
and delete files are immutable once written, and changes create new files plus
new catalog snapshots. Immutable files mean cached objects can never be stale,
so cache invalidation reduces to eviction and the delete path. The only
mutable state is the catalog (a SQL database), which is not on the object
storage path at all.

## Part 1: foyer as a write-through cache for DuckLake

### Architecture

```
DuckDB / DuckLake                    (SQL: ATTACH ... DATA_PATH 's3://lake/data/')
        │
        ▼
duckdb-opendal extension             (registers OpenDAL operators as DuckDB filesystems)
        │
        ▼
opendal Operator
        ├── FoyerLayer ──► foyer HybridCache
        │                    ├── memory tier (hot Parquet objects)
        │                    └── disk tier   (NVMe, e.g. 100s of GB)
        └── RetryLayer, MetricsLayer, ...
        │
        ▼
object storage (S3 / GCS / ...)
```

Every DuckLake data-file write goes through the operator to object storage and
lands in the foyer cache on success; every read is served from memory or local
NVMe when possible. Because the demo in this directory already proves DuckLake
runs all data-file IO through the duckdb-opendal operator, inserting
`FoyerLayer` into that operator stack transparently gives DuckLake a
write-through cache with zero changes on the DuckLake side.

### The Rust composition

```rust
use foyer::{BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCacheBuilder};
use opendal::layers::{FoyerLayer, RetryLayer};
use opendal::{services::S3, Operator};

let device = FsDeviceBuilder::new("/mnt/nvme/lake-cache")
    .with_capacity(200 * 1024 * 1024 * 1024)
    .build()?;

let cache = HybridCacheBuilder::new()
    .memory(4 * 1024 * 1024 * 1024)
    .storage()
    .with_engine_config(BlockEngineConfig::new(device))
    .build()
    .await?;

let op = Operator::new(S3::default().bucket("lake"))?
    .layer(FoyerLayer::new(cache).with_size_limit(64 * 1024 * 1024))
    .layer(RetryLayer::new())
    .finish();
```

### Where the layer can live

- **Inside the duckdb-opendal extension (ideal for the SQL-only path).** The
  extension currently builds the operator from `CREATE SECRET` config
  (`io_config`, `retry_config`); it does not yet expose a foyer cache option.
  Since `FoyerLayer` is first-party in the opendal crate the extension
  already embeds, adding a `cache_config MAP {...}` to the secret that wires
  up a `HybridCache` + `FoyerLayer` is a natural, small upstream contribution.
- **In your own Rust services (available today).** Anywhere you own the
  `Operator` — an ETL writer producing DuckLake Parquet files, a compaction
  job, a query sidecar — compose `FoyerLayer` as above with no upstream
  changes needed.

### Caveats

- **Whole-object caching vs ranged reads.** `FoyerLayer` caches objects, and
  DuckDB reads Parquet with ranged requests (footer, then row groups). A miss
  on a huge file either bypasses the cache (size limit) or fetches the whole
  object. Tune DuckLake's target file size and the layer's
  `with_size_limit` together; RisingWave's block-granular design on foyer is
  the reference for finer-grained caching if object granularity is too coarse.
- **Write-through ≠ write-back.** Writes still pay full object-storage
  latency; the benefit is that freshly written files are immediately warm for
  readers in the same process (cache hydration). DuckLake's own **data
  inlining** (small writes buffered in the catalog, flushed later via
  `ducklake_flush_inlined_data`) is the complementary write-side optimization
  and works with any of this unchanged.
- **The cache helps the process that owns it.** Foyer is embedded and
  per-process; other DuckDB clients don't see it. That is Part 2.
- **Read-only alternatives on the pure-DuckDB side**: the `cache_httpfs`
  community extension (block-level read cache over httpfs) and
  `duckdb-diskcache` (disk spill for DuckDB's built-in ExternalFileCache,
  aimed at DuckLake/Iceberg/Delta). Neither is write-through; both are useful
  comparisons for read-heavy workloads.

## Part 2: a cache service on a separate server

**Foyer itself cannot be that service** — it is strictly an embedded library
with no server mode, no network protocol, and no multi-node coherence. But it
is the standard engine to build such a service from, and prior art exists:

- **[Cachey](https://github.com/s2-streamstore/cachey)** (by S2): a
  self-contained single-node HTTP **read-through** cache for object storage,
  powered by a foyer hybrid cache, with a `/fetch` API supporting precise byte
  ranges against any S3-compatible backend.
- **Percas**: a distributed persistent cache service built on foyer.

### Recommended shape: an S3-compatible caching proxy

Because DuckLake's `DATA_PATH` (or a duckdb-opendal `s3` secret with a custom
`endpoint`) can point at any S3-compatible endpoint, the cleanest cache
service is a small Rust proxy that speaks the S3 API on the front and uses
exactly the Part 1 operator stack on the back:

```
DuckDB #1 ─┐
DuckDB #2 ─┼── s3://lake/... (endpoint = cache-service:9000)
ETL writer ┘        │
                    ▼
        cache service (one per rack/AZ)
        S3-compatible frontend (GET/PUT/DELETE/LIST)
                    │
        opendal Operator + FoyerLayer(HybridCache: RAM + NVMe)
                    │
                    ▼
            real object storage
```

- `GET` → foyer hit from RAM/NVMe, miss fetched from origin and admitted.
- `PUT` → forwarded to origin, admitted to cache on success (write-through);
  the object is durable in real object storage before the client sees OK.
- `DELETE` → forwarded and invalidated.
- All DuckDB clients pointing at the service share one warm cache, so a file
  written by the ETL pipeline is a cache hit for every subsequent reader —
  and DuckLake's immutable files mean entries never go stale.

Practical notes:

- Only proxy the **data path**. The DuckLake catalog stays on Postgres and
  must not go through the cache.
- Range requests matter: serve `GET` with `Range` headers out of cached
  objects (foyer stores the object; the proxy slices it), or cache at block
  granularity keyed by `(path, block_index)` as RisingWave does.
- Because writes are write-through, the service is a performance tier, not a
  durability tier: on cache-server loss, clients can fail over to the real
  object-storage endpoint with nothing lost.
- If write-through on the service is not required, Cachey can be deployed
  today as the read side, with writers going straight to object storage.
- A multi-node variant adds consistent hashing over cache nodes (what Percas
  provides); DuckLake's immutability makes this easy since there is no
  coherence protocol to build — a key is either present or fetched from
  origin.

## Sources

- OpenDAL layers: https://docs.rs/opendal/latest/opendal/layers/index.html
- FoyerLayer: https://docs.rs/opendal/latest/opendal/layers/struct.FoyerLayer.html
- OpenDAL cache discussions: https://github.com/apache/opendal/discussions/2953,
  https://github.com/apache/opendal/issues/5678
- Foyer: https://github.com/foyer-rs/foyer, https://foyer.rs
- RisingWave hybrid cache design:
  https://risingwave.com/blog/the-case-for-hybrid-cache-for-object-stores/
- SlateDB cache consolidation discussion:
  https://github.com/slatedb/slatedb/issues/1544
- Cachey: https://github.com/s2-streamstore/cachey
- cache_httpfs: https://duckdb.org/community_extensions/extensions/cache_httpfs
- duckdb-diskcache: https://github.com/peterboncz/duckdb-diskcache
- DuckLake storage model: https://ducklake.select/docs/stable/duckdb/usage/choosing_storage
- MotherDuck OLAP caching survey: https://motherduck.com/blog/duckdb-olap-caching/
