"""A separate process for test_write_concurrency.py: writes or reads one delta_classic database and reports as JSON.

Usage: concurrency_worker.py <extension> <directory> <role> <worker id> <arguments as JSON>
"""

import json
import os
import random
import sys
import time

import duckdb

BATCH = 1000


def connect(extension):
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute(f"LOAD '{extension}'")
    con.execute("INSTALL delta")
    con.execute("LOAD delta")
    return con


def append(con, worker, args):
    """Appends whole batches, retrying a batch that lost the race for a Delta version."""
    conflicts = 0
    for k in range(args["batches"]):
        batch = worker * args["batches"] + k
        while True:
            try:
                con.execute(
                    f"INSERT INTO db.{args['table']} SELECT {batch} * {BATCH} + range, {batch}, {worker} "
                    f"FROM range({BATCH})"
                )
                break
            except duckdb.Error as error:
                if "conflict" not in str(error):
                    raise
                conflicts += 1
                if conflicts > 500:
                    raise
                time.sleep(random.uniform(0, 0.05))
    return {"conflicts": conflicts}


def read(con, worker, args):
    """Reads until the stop file appears, checking that every read is made of whole batches."""
    reads = 0
    violations = []
    deadline = time.time() + 300
    while time.time() < deadline:
        done = os.path.exists(args["stop_file"])
        rows, bad_batches, distinct_ids = con.execute(
            f"SELECT count(*), "
            f"(SELECT count(*) FROM (SELECT batch FROM db.{args['table']} GROUP BY batch "
            f"HAVING count(*) != {BATCH} OR min(id) != batch * {BATCH} OR max(id) != batch * {BATCH} + {BATCH - 1})), "
            f"count(DISTINCT id) FROM db.{args['table']}"
        ).fetchone()
        reads += 1
        if rows % BATCH != 0 or bad_batches != 0 or distinct_ids != rows:
            violations.append({"rows": rows, "bad_batches": bad_batches, "distinct_ids": distinct_ids})
        if done:
            break
    return {"reads": reads, "violations": violations}


def create(con, worker, args):
    """Creates a table, and fills it if asked; reports the error instead of raising it."""
    try:
        con.execute(f"CREATE TABLE db.{args['table']} (id INTEGER, worker INTEGER)")
    except duckdb.Error as error:
        return {"created": False, "error": str(error)}
    if args.get("rows"):
        con.execute(f"INSERT INTO db.{args['table']} SELECT range, {worker} FROM range({args['rows']})")
    return {"created": True}


def main():
    extension, directory, role, worker, args = sys.argv[1:6]
    worker = int(worker)
    args = json.loads(args)
    con = connect(extension)
    con.execute(f"ATTACH '{directory}' AS db (TYPE delta_classic, READ_ONLY {'true' if role == 'read' else 'false'})")
    # Start together, so the writers really race
    start = args.get("start_at")
    if start:
        time.sleep(max(0.0, start - time.time()))
    result = {"append": append, "read": read, "create": create}[role](con, worker, args)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
