"""Concurrent writers in separate processes and threads must never corrupt, lose or duplicate data.

Writers race for the next Delta version: the loser of a race gets a commit conflict and retries, so every batch lands
exactly once. Readers run alongside and check that every read is made of whole batches. Afterwards the Delta log is
audited directly, and the table is read by delta-rs as an independent implementation.
"""

import glob
import json
import os
import subprocess
import sys
import threading
import time

import duckdb
import pytest

WORKER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "concurrency_worker.py")
BATCH = 1000


def connect(extension_path):
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute(f"LOAD '{extension_path}'")
    con.execute("INSTALL delta")
    con.execute("LOAD delta")
    return con


def spawn(extension_path, directory, role, worker, args):
    return subprocess.Popen(
        [sys.executable, WORKER, extension_path, str(directory), role, str(worker), json.dumps(args)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def finish(process, timeout=600):
    out, err = process.communicate(timeout=timeout)
    assert process.returncode == 0, f"worker failed:\n{err}"
    return json.loads(out.strip().splitlines()[-1])


def read_log(table_path):
    """The commits of a Delta table, in version order, as lists of actions."""
    commits = []
    for path in sorted(glob.glob(os.path.join(table_path, "_delta_log", "*.json"))):
        with open(path) as f:
            commits.append([json.loads(line) for line in f if line.strip()])
    return commits


def audit_log(table_path):
    """Checks the log is gapless and that the live files are exactly the referenced ones. Returns the live files."""
    commits = read_log(table_path)
    versions = sorted(
        int(os.path.basename(p).split(".")[0]) for p in glob.glob(os.path.join(table_path, "_delta_log", "*.json"))
    )
    assert versions == list(range(len(versions))), f"gaps in the Delta log: {versions}"
    live = set()
    for actions in commits:
        for action in actions:
            if "add" in action:
                live.add(action["add"]["path"])
            if "remove" in action:
                live.discard(action["remove"]["path"])
    for path in live:
        assert os.path.exists(os.path.join(table_path, path)), f"the log references a missing file: {path}"
    return commits, live


def data_files(table_path):
    """The data files on disk (checkpoints in _delta_log are not data files)."""
    files = set()
    for path in glob.glob(os.path.join(table_path, "**", "*.parquet"), recursive=True):
        relative = os.path.relpath(path, table_path).replace(os.sep, "/")
        if not relative.startswith("_delta_log/"):
            files.add(relative)
    return files


def check_batches(con, table, expected_batches):
    rows, distinct_ids, bad_batches, batches = con.execute(
        f"SELECT count(*), count(DISTINCT id), "
        f"(SELECT count(*) FROM (SELECT batch FROM {table} GROUP BY batch "
        f"HAVING count(*) != {BATCH} OR min(id) != batch * {BATCH} OR max(id) != batch * {BATCH} + {BATCH - 1})), "
        f"count(DISTINCT batch) FROM {table}"
    ).fetchone()
    assert bad_batches == 0, "a batch is partial or misplaced"
    assert distinct_ids == rows, "a row is duplicated"
    assert batches == expected_batches and rows == expected_batches * BATCH, f"{batches} batches, {rows} rows"


@pytest.mark.parametrize("writers,batches", [(6, 5)])
def test_processes_append_concurrently(extension_path, tmp_path, writers, batches):
    directory = tmp_path / "lake"
    con = connect(extension_path)
    con.execute(f"ATTACH '{directory}' AS w (TYPE delta_classic, READ_ONLY false)")
    con.execute("CREATE TABLE w.main.events (id BIGINT, batch INTEGER, worker INTEGER)")
    con.execute("DETACH w")

    stop_file = str(tmp_path / "stop")
    start_at = time.time() + 5
    readers = [
        spawn(extension_path, directory, "read", r, {"table": "main.events", "stop_file": stop_file}) for r in range(2)
    ]
    procs = [
        spawn(
            extension_path, directory, "append", w, {"table": "main.events", "batches": batches, "start_at": start_at}
        )
        for w in range(writers)
    ]
    results = [finish(p) for p in procs]
    open(stop_file, "w").close()
    reads = [finish(p) for p in readers]

    print(f"\nconflicts retried: {sum(r['conflicts'] for r in results)}, reads: {sum(r['reads'] for r in reads)}")
    for r in reads:
        assert r["violations"] == [], f"a reader saw partial data: {r['violations'][:5]}"
        assert r["reads"] > 0

    # Every batch exactly once
    con.execute(f"ATTACH '{directory}' AS r (TYPE delta_classic)")
    check_batches(con, "r.main.events", writers * batches)

    # One commit per batch, each adding exactly one batch's rows, and nothing else in the log
    table_path = str(directory / "events")
    commits, live = audit_log(table_path)
    assert len(commits) == 1 + writers * batches
    for version, actions in enumerate(commits[1:], start=1):
        added = [a["add"] for a in actions if "add" in a]
        assert added, f"version {version} adds no file"
        records = sum(json.loads(a["stats"])["numRecords"] for a in added if a.get("stats"))
        if all(a.get("stats") for a in added):
            assert records == BATCH, f"version {version} adds {records} rows"
        assert not any("remove" in a for a in actions), f"version {version} removes files"

    # Losers of a race cleaned up what they staged
    orphans = data_files(table_path) - live
    assert not orphans, f"files not referenced by the log: {sorted(orphans)[:5]}"

    # Every version reads as the batches committed up to it
    for version in range(len(commits)):
        count = con.execute(f"SELECT count(*) FROM r.main.events AT (VERSION => {version})").fetchone()[0]
        assert count == version * BATCH, f"version {version} has {count} rows"


def test_threads_append_concurrently(extension_path, tmp_path):
    """Threads of one process, each on its own connection, through one attached database."""
    directory = tmp_path / "lake"
    con = connect(extension_path)
    con.execute(f"ATTACH '{directory}' AS db (TYPE delta_classic, READ_ONLY false)")
    con.execute("CREATE TABLE db.main.events (id BIGINT, batch INTEGER, worker INTEGER)")

    threads, batches = 8, 4
    errors = []
    conflicts = [0]

    def write(worker):
        cursor = con.cursor()
        try:
            for k in range(batches):
                batch = worker * batches + k
                while True:
                    try:
                        cursor.execute(
                            f"INSERT INTO db.main.events SELECT {batch} * {BATCH} + range, {batch}, {worker} FROM range({BATCH})"
                        )
                        break
                    except duckdb.Error as error:
                        if "conflict" not in str(error):
                            raise
                        conflicts[0] += 1
                # Read in between: never anything partial
                rows = cursor.execute("SELECT count(*) FROM db.main.events").fetchone()[0]
                assert rows % BATCH == 0
        except BaseException as error:  # noqa: B036 - reported to the main thread
            errors.append(error)

    pool = [threading.Thread(target=write, args=(w,)) for w in range(threads)]
    for t in pool:
        t.start()
    for t in pool:
        t.join()
    assert not errors, errors
    print(f"\nconflicts retried: {conflicts[0]}")
    check_batches(con, "db.main.events", threads * batches)
    _, live = audit_log(str(directory / "events"))
    assert not data_files(str(directory / "events")) - live


def test_processes_create_same_table(extension_path, tmp_path):
    """Processes creating the same table: exactly one creates it, the others fail cleanly."""
    directory = tmp_path / "lake"
    (directory / "sales").mkdir(parents=True)
    # A schema directory needs a table to be discovered as one
    con = connect(extension_path)
    con.execute(f"ATTACH '{directory / 'sales' / 'seed'}' AS seed (TYPE delta)")
    con.execute("CREATE TABLE seed.seed (id INTEGER)")
    con.execute("DETACH seed")

    start_at = time.time() + 5
    procs = [
        spawn(extension_path, directory, "create", w, {"table": "sales.same", "start_at": start_at}) for w in range(6)
    ]
    results = [finish(p) for p in procs]
    created = [r for r in results if r["created"]]
    assert len(created) == 1, results
    for r in results:
        if not r["created"]:
            assert "already exists" in r["error"] or "conflict" in r["error"], r["error"]

    commits, _ = audit_log(str(directory / "sales" / "same"))
    assert len(commits) == 1
    con.execute(f"ATTACH '{directory}' AS r (TYPE delta_classic)")
    assert con.execute("SELECT count(*) FROM r.sales.same").fetchone()[0] == 0
    assert (
        con.execute(
            "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'r' AND table_name = 'same'"
        ).fetchone()[0]
        == 1
    )


def test_processes_create_different_tables(extension_path, tmp_path):
    """Processes each creating and filling their own table in one schema."""
    directory = tmp_path / "lake"
    con = connect(extension_path)
    con.execute(f"ATTACH '{directory / 'sales' / 'seed'}' AS seed (TYPE delta)")
    con.execute("CREATE TABLE seed.seed (id INTEGER)")
    con.execute("DETACH seed")

    start_at = time.time() + 5
    procs = [
        spawn(
            extension_path,
            directory,
            "create",
            w,
            {"table": f"sales.t_{w}", "rows": 100 * (w + 1), "start_at": start_at},
        )
        for w in range(6)
    ]
    results = [finish(p) for p in procs]
    assert all(r["created"] for r in results), results

    con.execute(f"ATTACH '{directory}' AS r (TYPE delta_classic)")
    for w in range(6):
        rows, workers = con.execute(f"SELECT count(*), count(DISTINCT worker) FROM r.sales.t_{w}").fetchone()
        assert rows == 100 * (w + 1) and workers == 1


def test_delta_rs_reads_concurrent_writes(extension_path, tmp_path):
    """delta-rs, an independent implementation, reads exactly what concurrent writers committed."""
    deltalake = pytest.importorskip("deltalake")
    directory = tmp_path / "lake"
    con = connect(extension_path)
    con.execute(f"ATTACH '{directory}' AS w (TYPE delta_classic, READ_ONLY false)")
    con.execute("CREATE TABLE w.main.events (id BIGINT, batch INTEGER, worker INTEGER)")
    con.execute("DETACH w")

    start_at = time.time() + 5
    procs = [
        spawn(extension_path, directory, "append", w, {"table": "main.events", "batches": 3, "start_at": start_at})
        for w in range(4)
    ]
    for p in procs:
        finish(p)

    table = deltalake.DeltaTable(str(directory / "events"))
    assert table.version() == 12
    ids = table.to_pyarrow_table(columns=["id"]).column("id").to_pylist()
    assert sorted(ids) == list(range(12 * BATCH))
