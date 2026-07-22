"""Proof of concept: run Supabase ETL (etl-replicator) inside a MotherDuck flight.

Pipeline shape:
  local Postgres (source, wal_level=logical)
    -> etl-replicator (pgoutput logical replication, DuckLake destination)
    -> DuckLake catalog in the same local Postgres + Parquet data on local MinIO (S3 API)

Binary provisioning:
  1. git-clone the fork branch and use the cached compiled binary if present + runnable.
  2. Otherwise install rustup + toolchain and compile in the flight, then (if a
     github token secret is configured) push the fresh cache back to the fork.

The flight inserts rows into Postgres periodically while the replicator runs,
then verifies the DuckLake table matches the source table.
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
INSERT_ROUNDS = int(os.environ.get("INSERT_ROUNDS", "6"))
INSERT_INTERVAL_SECS = int(os.environ.get("INSERT_INTERVAL_SECS", "15"))
VERIFY_TIMEOUT_SECS = int(os.environ.get("VERIFY_TIMEOUT_SECS", "180"))

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


def compile_in_flight():
    log("=== no usable cached binary: installing rust and compiling (slow path) ===")
    sh(
        "DEBIAN_FRONTEND=noninteractive apt-get install -y -qq build-essential pkg-config cmake 2>&1 | tail -1",
        timeout=900,
    )
    sh("curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal", timeout=1200)
    cargo_env = 'source "$HOME/.cargo/env" && '
    sh(
        f"{cargo_env} cd {SRC_DIR} && cargo build --release -p etl-replicator "
        "--no-default-features --features ducklake 2>&1 | tail -20",
        timeout=7200,
    )
    sh(f"cp {SRC_DIR}/target/release/etl-replicator {BIN_PATH} && chmod +x {BIN_PATH}")
    if not binary_works():
        raise RuntimeError("freshly compiled binary does not run")
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


def psql(db, sql, check=True):
    path = f"{WORK}/cmd.sql"
    with open(path, "w") as f:
        f.write(sql)
    os.chmod(path, 0o644)
    return sh(f'su postgres -c "psql -v ON_ERROR_STOP=1 -d {db} -f {path}"', check=check)


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
        create publication {PUBLICATION} for table public.orders;
    """)
    log(f"wal_level={psql_value('postgres', 'show wal_level;')}")


def setup_minio():
    log("=== starting MinIO (local S3) ===")
    sh(f"curl -sSL -o {WORK}/minio https://dl.min.io/server/minio/release/linux-amd64/minio && chmod +x {WORK}/minio", timeout=600)
    sh(f"curl -sSL -o {WORK}/mc https://dl.min.io/client/mc/release/linux-amd64/mc && chmod +x {WORK}/mc", timeout=600)
    spawn("minio", f"{WORK}/minio server {WORK}/minio-data --address {MINIO_ADDR} --quiet",
          env={"MINIO_ROOT_USER": MINIO_USER, "MINIO_ROOT_PASSWORD": MINIO_PASS})
    for _ in range(30):
        r = subprocess.run(f"curl -s -o /dev/null -w '%{{http_code}}' http://{MINIO_ADDR}/minio/health/ready",
                           shell=True, capture_output=True, text=True)
        if r.stdout.strip() == "200":
            break
        time.sleep(1)
    else:
        raise RuntimeError("minio did not become ready")
    sh(f"{WORK}/mc alias set local http://{MINIO_ADDR} {MINIO_USER} {MINIO_PASS} && {WORK}/mc mb local/{BUCKET}")


def write_replicator_config():
    cfg_dir = f"{WORK}/config"
    os.makedirs(cfg_dir, exist_ok=True)
    open(f"{cfg_dir}/base.yaml", "w").close()
    prod = f"""
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
    with open(f"{cfg_dir}/prod.yaml", "w") as f:
        f.write(prod)
    log(f"replicator config written to {cfg_dir}")
    return cfg_dir


def seed_initial_rows():
    log("=== seeding initial rows (exercises initial COPY phase) ===")
    psql(SOURCE_DB, """
        insert into public.orders (id, customer, amount)
        select g, 'seed-customer-' || (g % 50), round((random() * 500)::numeric, 2)
        from generate_series(1, 1000) g;
    """)


def start_replicator(cfg_dir):
    log("=== starting etl-replicator ===")
    return spawn("replicator", BIN_PATH, env={
        "APP_CONFIG_DIR": cfg_dir,
        "APP_ENVIRONMENT": "prod",
        "RUST_LOG": os.environ.get("RUST_LOG", "info"),
    })


def periodic_inserts(rep):
    log(f"=== periodic writes: {INSERT_ROUNDS} rounds every {INSERT_INTERVAL_SECS}s ===")
    next_id = 1001
    for round_no in range(1, INSERT_ROUNDS + 1):
        time.sleep(INSERT_INTERVAL_SECS)
        if rep.poll() is not None:
            raise RuntimeError(f"replicator exited early with code {rep.returncode}")
        lo, hi = next_id, next_id + 99
        psql(SOURCE_DB, f"""
            insert into public.orders (id, customer, amount)
            select g, 'round{round_no}-customer-' || (g % 20), round((random() * 100)::numeric, 2)
            from generate_series({lo}, {hi}) g;
            update public.orders set status = 'updated-r{round_no}', amount = amount + 1
            where id % 97 = {round_no};
            delete from public.orders where id % 251 = {round_no};
        """, check=True)
        cnt = psql_value(SOURCE_DB, "select count(*) from public.orders;")
        log(f"round {round_no}: inserted {lo}..{hi}, source row count now {cnt}")
        next_id = hi + 1


def source_state():
    row = psql_value(SOURCE_DB, "select count(*) || '|' || coalesce(sum(id),0) || '|' || coalesce(sum(amount),0) from public.orders;")
    return row


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

    expected = source_state()
    deadline = time.time() + VERIFY_TIMEOUT_SECS
    got = None
    while time.time() < deadline:
        try:
            got = con.execute(
                "select count(*) || '|' || coalesce(sum(id),0) || '|' || coalesce(sum(amount),0) from lake.public.orders"
            ).fetchone()[0]
        except Exception as e:
            log(f"lake table not readable yet: {e}")
            got = None
        expected = source_state()
        if got == expected:
            break
        log(f"waiting for convergence: source={expected} lake={got}")
        time.sleep(5)

    print("\n================ RESULT ================", flush=True)
    log(f"source (count|sum(id)|sum(amount)): {expected}")
    log(f"lake   (count|sum(id)|sum(amount)): {got}")
    ok = got == expected
    log("REPLICATION VERIFIED ✅" if ok else "MISMATCH ❌")

    for label, q in [
        ("sample replicated rows", "select * from lake.public.orders order by id limit 5"),
        ("updated rows made it", "select count(*) from lake.public.orders where status like 'updated-%'"),
        ("ducklake snapshots", "select count(*) from lake.snapshots()"),
    ]:
        try:
            print(f"--- {label} ---\n{con.execute(q).fetchall()}", flush=True)
        except Exception as e:
            log(f"{label} failed: {e}")
    objs = sh(f"{WORK}/mc ls --recursive local/{BUCKET} | tail -5; {WORK}/mc du local/{BUCKET}", check=False)
    return ok


def cleanup():
    log("=== cleanup ===")
    for name, p in reversed(procs):
        if p.poll() is None:
            log(f"terminating {name}")
            p.terminate()
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
    if not fetch_cached_binary():
        compile_in_flight()
    setup_postgres()
    setup_minio()
    cfg_dir = write_replicator_config()
    seed_initial_rows()
    rep = start_replicator(cfg_dir)
    try:
        periodic_inserts(rep)
        ok = verify_ducklake()
        if not ok:
            raise RuntimeError("DuckLake state did not converge to source state")
        log("PoC finished successfully")
    finally:
        cleanup()


if __name__ == "__main__":
    main()
