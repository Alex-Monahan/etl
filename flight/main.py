"""Proof of concept: run Supabase ETL (etl-replicator) inside a MotherDuck flight.

Pipeline shape:
  local Postgres (source, wal_level=logical)
    -> etl-replicator (pgoutput logical replication, DuckLake destination)
    -> DuckLake catalog in the same local Postgres + Parquet data on local MinIO (S3 API)

Binary provisioning:
  1. git-clone the fork branch and use the cached compiled binary if present + runnable.
  2. Otherwise install rustup + toolchain and compile in the flight, then (if a
     github token secret is configured) push the fresh cache back to the fork.

Workload/test matrix exercised while the replicator runs:
  - initial COPY of pre-seeded rows
  - mixed insert/update/delete rounds
  - small single-row transactions (DuckLake data-inlining path)
  - one large multi-thousand-row transaction
  - TOAST columns: ~100KB text bodies, title-only updates (unchanged-TOAST),
    body rewrites, deletes
  - NULLs, unicode text, jsonb values
  - live schema changes: add column with default, add+drop column, rename column
  - TRUNCATE on a dedicated table

Verification compares dialect-safe integer fingerprints (counts, id sums,
cents-scaled amount sums, text lengths) between source Postgres and DuckLake,
plus source/lake column-list equality after DDL.
"""

import os
import shutil
import subprocess
import sys
import threading
import time

REPO_URL = os.environ.get("REPO_URL", "https://github.com/Alex-Monahan/etl")
CACHE_BRANCH = os.environ.get("CACHE_BRANCH", "claude/ducklake-replication-eval-l3dq8u")
CACHE_DIR_IN_REPO = "flight-cache"
BINARY_GZ = "etl-replicator-x86_64-unknown-linux-gnu.gz"

WORK = "/tmp/etl-poc"
SRC_DIR = f"{WORK}/etl"
BIN_PATH = f"{WORK}/etl-replicator"
PG_PASSWORD = "etlpoc"
SOURCE_DB = "sourcedb"
CATALOG_DB = "ducklake_catalog"
PUBLICATION = "etl_poc_pub"
MINIO_USER = "minioadmin"
MINIO_PASS = "minioadmin"
# NOTE: the replicator's Prometheus exporter hardcodes port 9000, keep MinIO off it.
MINIO_ADDR = "127.0.0.1:9100"
BUCKET = "etl-poc"
DATA_PATH = f"s3://{BUCKET}/lake"
INSERT_ROUNDS = int(os.environ.get("INSERT_ROUNDS", "3"))
INSERT_INTERVAL_SECS = int(os.environ.get("INSERT_INTERVAL_SECS", "10"))
LARGE_INSERT_ROWS = int(os.environ.get("LARGE_INSERT_ROWS", "50000"))
VERIFY_TIMEOUT_SECS = int(os.environ.get("VERIFY_TIMEOUT_SECS", "300"))

procs = []


def log(msg):
    print(f"[poc {time.strftime('%H:%M:%S')}] {msg}", flush=True)


def sh(cmd, check=True, timeout=600, quiet=False, **kw):
    if not quiet:
        log(f"$ {cmd}")
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout, **kw)
    out = (r.stdout + r.stderr).strip()
    if out and not quiet:
        print(out[-4000:], flush=True)
    if check and r.returncode != 0:
        raise RuntimeError(f"command failed ({r.returncode}): {cmd}\n{out[-4000:]}")
    return out


def spawn(name, cmd, env=None):
    log(f"starting {name}: {cmd}")
    full_env = dict(os.environ)
    if env:
        full_env.update(env)
    p = subprocess.Popen(
        cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, env=full_env,
    )
    procs.append((name, p))

    def pump():
        for line in p.stdout:
            print(f"[{name}] {line.rstrip()}", flush=True)

    threading.Thread(target=pump, daemon=True).start()
    return p


def env_recon():
    log("=== environment recon ===")
    sh("uname -m; cat /etc/os-release | head -3; ldd --version | head -1; nproc; df -h /tmp | tail -1; id", check=False)


def apt_install():
    log("=== installing system packages (git, postgres, curl) ===")
    sh("apt-get update -qq", timeout=600)
    sh(
        "DEBIAN_FRONTEND=noninteractive apt-get install -y -qq git curl ca-certificates "
        "postgresql libstdc++6 procps 2>&1 | tail -2",
        timeout=900,
    )
    sh("git --version && psql --version")


def clone_repo():
    log("=== cloning fork branch (with binary cache if present) ===")
    shutil.rmtree(SRC_DIR, ignore_errors=True)
    sh(f"git clone --depth 1 --branch {CACHE_BRANCH} {REPO_URL} {SRC_DIR}", timeout=900)
    sh(f"cd {SRC_DIR} && git log --oneline -1 && ls {CACHE_DIR_IN_REPO} 2>/dev/null || true")


def binary_works():
    """A cached binary 'works' if the dynamic loader accepts it: running it
    without config must produce the replicator's own config error, not a
    loader/GLIBC error."""
    if not os.path.exists(BIN_PATH):
        return False
    try:
        r = subprocess.run(
            [BIN_PATH], capture_output=True, text=True, timeout=60,
            env={**os.environ, "APP_CONFIG_DIR": "/nonexistent-poc-probe"},
        )
    except OSError as e:
        # e.g. Exec format error when the cached binary targets another arch.
        log(f"cached binary not executable on this runtime: {e}")
        return False
    out = r.stdout + r.stderr
    if "GLIBC" in out or ("not found" in out.lower() and "libc" in out.lower()):
        log(f"cached binary incompatible with this runtime:\n{out[-1500:]}")
        return False
    log(f"cached binary loads (exit {r.returncode}); sample output:\n{out[-600:]}")
    return True


def fetch_cached_binary():
    gz = f"{SRC_DIR}/{CACHE_DIR_IN_REPO}/{BINARY_GZ}"
    if not os.path.exists(gz):
        log("no cached binary in repo branch")
        return False
    sh(f"gunzip -c {gz} > {BIN_PATH} && chmod +x {BIN_PATH}")
    manifest = f"{SRC_DIR}/{CACHE_DIR_IN_REPO}/manifest.json"
    if os.path.exists(manifest):
        sh(f"cat {manifest}", check=False)
    return binary_works()


LOCAL_CACHE = "/tmp/etl-poc-binary-cache"


def fetch_local_cache():
    """Container-level cache: a reused flight container keeps /tmp, so a
    previous run's compiled binary can be reused without git or a rebuild."""
    gz = f"{LOCAL_CACHE}/{BINARY_GZ}"
    if not os.path.exists(gz):
        return False
    log("found container-local binary cache")
    sh(f"gunzip -c {gz} > {BIN_PATH} && chmod +x {BIN_PATH}")
    return binary_works()


def save_local_cache():
    os.makedirs(LOCAL_CACHE, exist_ok=True)
    sh(f"gzip -1 -c {BIN_PATH} > {LOCAL_CACHE}/{BINARY_GZ}")
    log(f"saved binary to container-local cache {LOCAL_CACHE}")


def compile_in_flight():
    log("=== no usable cached binary: installing rust and compiling (slow path) ===")
    # clang + lld: the workspace .cargo/config.toml pins them as the linker.
    # libssl-dev: transitive openssl-sys build. libclang-dev: bindgen users.
    sh(
        "DEBIAN_FRONTEND=noninteractive apt-get install -y -qq build-essential pkg-config cmake "
        "clang lld libssl-dev libclang-dev 2>&1 | tail -1",
        timeout=900,
    )
    sh("curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal", timeout=1200)
    # /bin/sh is dash on Debian: no `source` builtin, so call cargo by path.
    # The rustup shim auto-installs the repo's pinned toolchain on first use.
    sh(
        f'cd {SRC_DIR} && "$HOME/.cargo/bin/cargo" build --release -p etl-replicator '
        "--no-default-features --features ducklake 2>&1 | tail -20",
        timeout=7200,
    )
    sh(f"cp {SRC_DIR}/target/release/etl-replicator {BIN_PATH} && chmod +x {BIN_PATH}")
    if not binary_works():
        raise RuntimeError("freshly compiled binary does not run")
    save_local_cache()
    push_cache()


def push_cache():
    """Push the compiled binary back to the fork as the cache for next runs.
    Needs a GitHub token provided as a MotherDuck flights secret (env
    github_pat_TOKEN or GITHUB_TOKEN)."""
    token = os.environ.get("github_pat_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if not token:
        log("no github token secret configured; skipping cache push "
            "(add a flights secret named github_pat with key TOKEN to enable)")
        return
    log("=== pushing compiled binary cache back to fork ===")
    cache_dir = f"{SRC_DIR}/{CACHE_DIR_IN_REPO}"
    os.makedirs(cache_dir, exist_ok=True)
    sh(f"gzip -9 -c {BIN_PATH} > {cache_dir}/{BINARY_GZ}")
    commit = sh(f"cd {SRC_DIR} && git rev-parse HEAD", quiet=True)
    glibc = sh("ldd --version | head -1", quiet=True, check=False)
    with open(f"{cache_dir}/manifest.json", "w") as f:
        import json
        json.dump({
            "source_commit": commit.strip(),
            "target": "x86_64-unknown-linux-gnu",
            "built_on_glibc": glibc.strip(),
            "features": "ducklake (no default destinations)",
            "built_by": "motherduck-flight",
        }, f, indent=2)
    push_url = REPO_URL.replace("https://", f"https://x-access-token:{token}@") + ".git"
    sh(f"cd {SRC_DIR} && git config user.email 'flight@example.com' && git config user.name 'ETL PoC Flight' "
       f"&& git add {CACHE_DIR_IN_REPO} && git commit -m 'chore: cache etl-replicator binary from flight build' "
       f"&& git push {push_url} HEAD:{CACHE_BRANCH}", timeout=900)


def pg_conf_dir():
    ver = sh("ls /etc/postgresql | head -1", quiet=True).strip()
    return ver, f"/etc/postgresql/{ver}/main"


def psql(db, sql, check=True, quiet=False):
    path = f"{WORK}/cmd.sql"
    with open(path, "w") as f:
        f.write(sql)
    os.chmod(path, 0o644)
    return sh(f'su postgres -c "psql -v ON_ERROR_STOP=1 -d {db} -f {path}"', check=check, quiet=quiet)


def psql_value(db, sql):
    path = f"{WORK}/q.sql"
    with open(path, "w") as f:
        f.write(sql)
    os.chmod(path, 0o644)
    return sh(f'su postgres -c "psql -tA -d {db} -f {path}"', quiet=True).strip()


def setup_postgres():
    log("=== configuring postgres (wal_level=logical) ===")
    ver, conf = pg_conf_dir()
    log(f"postgres major version: {ver}")
    sh(f"pg_ctlcluster {ver} main start", check=False)
    psql("postgres", f"alter user postgres password '{PG_PASSWORD}';")
    psql("postgres", "alter system set wal_level = logical;")
    psql("postgres", "alter system set max_wal_senders = 20;")
    psql("postgres", "alter system set max_replication_slots = 20;")
    sh(f"pg_ctlcluster {ver} main restart", timeout=120)
    # Reset any state left behind by a previous run in a reused container.
    psql("postgres", "select pg_drop_replication_slot(slot_name) from pg_replication_slots;", check=False)
    psql("postgres", f"drop database if exists {SOURCE_DB} with (force);", check=False)
    psql("postgres", f"drop database if exists {CATALOG_DB} with (force);", check=False)
    psql("postgres", f"create database {SOURCE_DB};")
    psql("postgres", f"create database {CATALOG_DB};")
    psql(SOURCE_DB, f"""
        create table public.orders (
            id bigint primary key,
            customer text not null,
            amount numeric(10,2) not null,
            status text not null default 'new',
            created_at timestamptz not null default now()
        );
        -- TOAST test table: body values are large enough to be toasted.
        create table public.documents (
            id bigint primary key,
            title text not null,
            body text not null,
            meta jsonb,
            created_at timestamptz not null default now()
        );
        create table public.truncate_me (
            id bigint primary key,
            note text
        );
        create publication {PUBLICATION}
            for table public.orders, public.documents, public.truncate_me;
    """)
    log(f"wal_level={psql_value('postgres', 'show wal_level;')}")


HAVE_MC = False


def _wait_http(url, codes, attempts=30):
    for _ in range(attempts):
        r = subprocess.run(f"curl -s -o /dev/null -w '%{{http_code}}' {url}",
                           shell=True, capture_output=True, text=True)
        if r.stdout.strip() in codes:
            return True
        time.sleep(1)
    return False


def setup_object_store():
    """Start a local S3 endpoint: MinIO preferred, moto-server as fallback for
    environments where dl.min.io is unreachable."""
    global HAVE_MC
    log("=== starting local S3 (MinIO, moto fallback) ===")
    sh(f"rm -rf {WORK}/minio-data")
    try:
        sh(f"curl -fsSL -o {WORK}/minio https://dl.min.io/server/minio/release/linux-amd64/minio && chmod +x {WORK}/minio", timeout=600)
        sh(f"head -c 4 {WORK}/minio | grep -q ELF", quiet=True)
        sh(f"curl -fsSL -o {WORK}/mc https://dl.min.io/client/mc/release/linux-amd64/mc && chmod +x {WORK}/mc", timeout=600)
        spawn("minio", f"{WORK}/minio server {WORK}/minio-data --address {MINIO_ADDR} --quiet",
              env={"MINIO_ROOT_USER": MINIO_USER, "MINIO_ROOT_PASSWORD": MINIO_PASS})
        if not _wait_http(f"http://{MINIO_ADDR}/minio/health/ready", {"200"}):
            raise RuntimeError("minio did not become ready")
        sh(f"{WORK}/mc alias set local http://{MINIO_ADDR} {MINIO_USER} {MINIO_PASS} && {WORK}/mc mb local/{BUCKET}")
        HAVE_MC = True
        return
    except Exception as e:
        log(f"minio unavailable ({e}); falling back to moto server")
    # --ignore-installed sidesteps distro-owned packages (e.g. Debian PyYAML)
    # that pip cannot uninstall; do not pipe, so failures are visible.
    sh("python3 -m pip install -q --ignore-installed PyYAML 'moto[server]' boto3", timeout=600)
    host, port = MINIO_ADDR.split(":")
    spawn("moto", f"python3 -m moto.server -H {host} -p {port}")
    if not _wait_http(f"http://{MINIO_ADDR}/", {"200", "403", "404"}):
        raise RuntimeError("moto server did not become ready")
    import boto3
    s3 = boto3.client("s3", endpoint_url=f"http://{MINIO_ADDR}",
                      aws_access_key_id=MINIO_USER, aws_secret_access_key=MINIO_PASS,
                      region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)
    log(f"moto S3 ready with bucket {BUCKET}")


def write_replicator_config():
    cfg_dir = f"{WORK}/config"
    os.makedirs(cfg_dir, exist_ok=True)
    open(f"{cfg_dir}/base.yaml", "w").close()
    cfg = f"""
destination:
  ducklake:
    catalog_url: "postgres://postgres:{PG_PASSWORD}@127.0.0.1:5432/{CATALOG_DB}"
    data_path: "{DATA_PATH}"
    s3_access_key_id: "{MINIO_USER}"
    s3_secret_access_key: "{MINIO_PASS}"
    s3_region: "us-east-1"
    s3_endpoint: "{MINIO_ADDR}"
    s3_url_style: "path"
    s3_use_ssl: false
pipeline:
  id: 1
  publication_name: "{PUBLICATION}"
  pg_connection:
    host: "127.0.0.1"
    port: 5432
    name: "{SOURCE_DB}"
    username: "postgres"
    password: "{PG_PASSWORD}"
    tls:
      trusted_root_certs: ""
      enabled: false
  batch:
    max_fill_ms: 1000
"""
    # dev environment => console logging (prod logs to rotating files under
    # ./logs, which is useless inside a flight).
    with open(f"{cfg_dir}/dev.yaml", "w") as f:
        f.write(cfg)
    log(f"replicator config written to {cfg_dir}")
    return cfg_dir


def seed_initial_rows():
    log("=== seeding initial rows (exercises initial COPY phase) ===")
    psql(SOURCE_DB, """
        insert into public.orders (id, customer, amount)
        select g, 'seed-customer-' || (g % 50), round((random() * 500)::numeric, 2)
        from generate_series(1, 1000) g;
        insert into public.truncate_me
        select g, 'pre-truncate-' || g from generate_series(1, 100) g;
    """)


def start_replicator(cfg_dir):
    log("=== starting etl-replicator ===")
    return spawn("replicator", BIN_PATH, env={
        "APP_CONFIG_DIR": cfg_dir,
        "APP_ENVIRONMENT": "dev",
        "RUST_LOG": os.environ.get("RUST_LOG", "info"),
    })


def check_alive(rep):
    if rep.poll() is not None:
        raise RuntimeError(f"replicator exited early with code {rep.returncode}")


def phase_mixed_rounds(rep):
    log(f"=== phase: mixed insert/update/delete rounds ({INSERT_ROUNDS} x {INSERT_INTERVAL_SECS}s) ===")
    next_id = 1001
    for round_no in range(1, INSERT_ROUNDS + 1):
        time.sleep(INSERT_INTERVAL_SECS)
        check_alive(rep)
        lo, hi = next_id, next_id + 99
        psql(SOURCE_DB, f"""
            insert into public.orders (id, customer, amount)
            select g, 'round{round_no}-customer-' || (g % 20), round((random() * 100)::numeric, 2)
            from generate_series({lo}, {hi}) g;
            update public.orders set status = 'updated-r{round_no}', amount = amount + 1
            where id % 97 = {round_no};
            delete from public.orders where id % 251 = {round_no};
        """, quiet=True)
        cnt = psql_value(SOURCE_DB, "select count(*) from public.orders;")
        log(f"round {round_no}: inserted {lo}..{hi}, source row count now {cnt}")
        next_id = hi + 1
    return next_id


def phase_small_inserts(rep, next_id):
    log("=== phase: small single-row transactions (data-inlining path) ===")
    check_alive(rep)
    for i in range(10):
        psql(SOURCE_DB, f"""
            insert into public.orders (id, customer, amount)
            values ({next_id + i}, 'tiny-txn-{i}', {i}.25);
        """, quiet=True)
    log(f"10 single-row inserts done ({next_id}..{next_id + 9})")
    return next_id + 10


def phase_large_insert(rep, next_id):
    log(f"=== phase: large single-transaction insert ({LARGE_INSERT_ROWS} rows) ===")
    check_alive(rep)
    lo, hi = next_id, next_id + LARGE_INSERT_ROWS - 1
    psql(SOURCE_DB, f"""
        insert into public.orders (id, customer, amount)
        select g, 'bulk-customer-' || (g % 1000), round((random() * 1000)::numeric, 2)
        from generate_series({lo}, {hi}) g;
    """, quiet=True)
    log(f"large insert committed: ids {lo}..{hi}")
    return hi + 1


def phase_toast(rep):
    log("=== phase: TOAST columns (large text bodies) ===")
    check_alive(rep)
    # ~96KB bodies (well past the ~2KB TOAST threshold), unicode titles,
    # jsonb with NULLs on odd rows.
    psql(SOURCE_DB, """
        insert into public.documents (id, title, body, meta)
        select g,
               'doc-café-日本語-' || g,
               repeat(md5(g::text), 3000),
               case when g % 2 = 0 then jsonb_build_object('k', g, 'tag', 'even') end
        from generate_series(1, 20) g;
    """, quiet=True)
    log("20 documents inserted with ~96KB bodies")
    time.sleep(3)
    # Title-only update: body is NOT sent by pgoutput (unchanged-toast) with
    # default replica identity — exercises ETL's partial-update handling.
    psql(SOURCE_DB, """
        update public.documents set title = title || '-retitled' where id % 2 = 1;
    """, quiet=True)
    log("title-only updates on odd ids (unchanged-TOAST path)")
    # Body rewrites and a delete.
    psql(SOURCE_DB, """
        update public.documents set body = repeat(md5('rewrite' || id::text), 3500) where id in (2, 4);
        delete from public.documents where id = 20;
    """, quiet=True)
    log("body rewrites on ids 2,4; deleted id 20")


def phase_schema_changes(rep):
    log("=== phase: live schema changes (DDL replication) ===")
    check_alive(rep)
    psql(SOURCE_DB, """
        alter table public.orders add column discount numeric(5,2) not null default 0;
    """)
    time.sleep(2)
    psql(SOURCE_DB, """
        update public.orders set discount = 5.25 where id % 100 = 0;
    """, quiet=True)
    log("added orders.discount with default 0, set 5.25 on id%100=0")
    psql(SOURCE_DB, """
        alter table public.orders add column scratch text;
    """)
    time.sleep(2)
    psql(SOURCE_DB, """
        alter table public.orders drop column scratch;
    """)
    log("added and dropped orders.scratch")
    psql(SOURCE_DB, """
        alter table public.orders rename column status to order_status;
    """)
    time.sleep(2)
    psql(SOURCE_DB, """
        update public.orders set order_status = 'updated-after-rename' where id % 500 = 0;
    """, quiet=True)
    log("renamed status -> order_status and wrote through the new name")


def phase_truncate(rep):
    log("=== phase: TRUNCATE ===")
    check_alive(rep)
    psql(SOURCE_DB, "truncate table public.truncate_me;")
    time.sleep(2)
    psql(SOURCE_DB, """
        insert into public.truncate_me
        select g, 'post-truncate-' || g from generate_series(1001, 1005) g;
    """, quiet=True)
    log("truncated truncate_me (100 rows) and inserted 5 post-truncate rows")


# Integer-only aggregates so Postgres and DuckDB render identical strings.
FINGERPRINT_QUERIES = [
    ("orders",
     "select count(*) || '|' || coalesce(sum(id),0)"
     " || '|' || coalesce(sum(cast(amount*100 as bigint)),0)"
     " || '|' || coalesce(sum(cast(discount*100 as bigint)),0)"
     " || '|' || count(*) filter (where order_status like 'updated-%')"
     " from {t}orders"),
    ("documents",
     "select count(*) || '|' || coalesce(sum(id),0)"
     " || '|' || coalesce(sum(length(title)),0)"
     " || '|' || coalesce(sum(length(body)),0)"
     " || '|' || count(meta)"
     " || '|' || count(*) filter (where title like '%-retitled')"
     " from {t}documents"),
    ("truncate_me",
     "select count(*) || '|' || coalesce(sum(id),0) from {t}truncate_me"),
]


def source_fingerprint():
    parts = []
    for name, q in FINGERPRINT_QUERIES:
        parts.append(f"{name}={psql_value(SOURCE_DB, q.format(t='public.') + ';')}")
    return "; ".join(parts)


def lake_fingerprint(con):
    parts = []
    for name, q in FINGERPRINT_QUERIES:
        val = con.execute(q.format(t="lake.public.")).fetchone()[0]
        parts.append(f"{name}={val}")
    return "; ".join(parts)


def source_columns(table):
    out = psql_value(
        SOURCE_DB,
        f"select string_agg(column_name, ',' order by column_name) from information_schema.columns "
        f"where table_schema = 'public' and table_name = '{table}';")
    return sorted(out.split(","))


def lake_columns(con, table):
    rows = con.execute(f"describe lake.public.{table}").fetchall()
    return sorted(r[0] for r in rows if not r[0].startswith("__etl_"))


def verify_ducklake():
    log("=== verifying DuckLake contents with duckdb ===")
    import duckdb
    con = duckdb.connect()
    con.execute("INSTALL ducklake; LOAD ducklake; INSTALL postgres; LOAD postgres; INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        CREATE OR REPLACE SECRET minio (
            TYPE S3, PROVIDER config,
            KEY_ID '{MINIO_USER}', SECRET '{MINIO_PASS}',
            REGION 'us-east-1', ENDPOINT '{MINIO_ADDR}',
            URL_STYLE 'path', USE_SSL false, URL_COMPATIBILITY_MODE true,
            SCOPE '{DATA_PATH}'
        );
    """)
    con.execute(f"""
        ATTACH 'ducklake:postgres:host=127.0.0.1 port=5432 dbname={CATALOG_DB} user=postgres password={PG_PASSWORD}'
        AS lake (DATA_PATH '{DATA_PATH}');
    """)

    deadline = time.time() + VERIFY_TIMEOUT_SECS
    expected = got = None
    while time.time() < deadline:
        expected = source_fingerprint()
        try:
            got = lake_fingerprint(con)
        except Exception as e:
            log(f"lake not fully readable yet: {str(e).splitlines()[0][:200]}")
            got = None
        if got == expected:
            break
        log(f"waiting for convergence:\n  source={expected}\n  lake  ={got}")
        time.sleep(5)

    print("\n================ RESULT ================", flush=True)
    log(f"source fingerprint: {expected}")
    log(f"lake   fingerprint: {got}")
    data_ok = got == expected
    log("DATA " + ("MATCH ✅" if data_ok else "MISMATCH ❌"))

    schema_ok = True
    for table in ("orders", "documents", "truncate_me"):
        try:
            src_cols, lk_cols = source_columns(table), lake_columns(con, table)
            ok = src_cols == lk_cols
            schema_ok &= ok
            log(f"schema {table}: {'MATCH ✅' if ok else f'MISMATCH ❌ source={src_cols} lake={lk_cols}'}")
        except Exception as e:
            schema_ok = False
            log(f"schema {table}: check failed: {e}")

    for label, q in [
        # created_at cast to varchar: timestamptz fetch needs pytz, not installed.
        ("sample orders rows (post-rename schema)",
         "select id, customer, amount, order_status, discount from lake.public.orders order by id limit 3"),
        ("toast integrity: per-row body lengths",
         "select id, length(title), length(body) from lake.public.documents order by id limit 6"),
        ("ducklake snapshots", "select count(*) from lake.snapshots()"),
    ]:
        try:
            print(f"--- {label} ---\n{con.execute(q).fetchall()}", flush=True)
        except Exception as e:
            log(f"{label} failed: {e}")
    if HAVE_MC:
        sh(f"{WORK}/mc du local/{BUCKET}; {WORK}/mc ls --recursive local/{BUCKET} | tail -5", check=False)
    return data_ok and schema_ok


def cleanup():
    log("=== cleanup ===")
    for name, p in reversed(procs):
        if p.poll() is None:
            log(f"terminating {name}")
            p.terminate()
            if name == "replicator":
                # Let the replicator finish graceful shutdown before its
                # source/destination Postgres goes away.
                for _ in range(20):
                    if p.poll() is not None:
                        break
                    time.sleep(1)
    time.sleep(3)
    for name, p in procs:
        if p.poll() is None:
            p.kill()
    ver, _ = pg_conf_dir()
    sh(f"pg_ctlcluster {ver} main stop", check=False)


def main():
    os.makedirs(WORK, exist_ok=True)
    env_recon()
    apt_install()
    clone_repo()
    if not fetch_cached_binary() and not fetch_local_cache():
        compile_in_flight()
    setup_postgres()
    setup_object_store()
    cfg_dir = write_replicator_config()
    seed_initial_rows()
    rep = start_replicator(cfg_dir)
    try:
        next_id = phase_mixed_rounds(rep)
        next_id = phase_small_inserts(rep, next_id)
        next_id = phase_large_insert(rep, next_id)
        phase_toast(rep)
        phase_schema_changes(rep)
        phase_truncate(rep)
        ok = verify_ducklake()
        if not ok:
            raise RuntimeError("DuckLake state did not converge to source state")
        log("PoC finished successfully — all test phases verified")
    finally:
        cleanup()


if __name__ == "__main__":
    main()
