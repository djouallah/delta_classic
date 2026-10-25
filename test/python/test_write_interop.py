"""Tables written through delta_classic are read the same by delta-rs, an independent Delta implementation, and
tables written by delta-rs can be appended to through delta_classic."""

import duckdb
import pytest

deltalake = pytest.importorskip("deltalake")
pa = pytest.importorskip("pyarrow")


def connect(extension_path):
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute(f"LOAD '{extension_path}'")
    con.execute("INSTALL delta")
    con.execute("LOAD delta")
    con.execute("SET TimeZone = 'UTC'")
    return con


def same_rows(con, left, right):
    """Whether two relations hold the same rows, compared as text so NULLs and nested values compare too."""
    return (
        con.execute(
            f"SELECT count(*) FROM ("
            f"(SELECT CAST(COLUMNS(*) AS VARCHAR) FROM {left} EXCEPT ALL SELECT CAST(COLUMNS(*) AS VARCHAR) FROM {right}) "
            f"UNION ALL "
            f"(SELECT CAST(COLUMNS(*) AS VARCHAR) FROM {right} EXCEPT ALL SELECT CAST(COLUMNS(*) AS VARCHAR) FROM {left}))"
        ).fetchone()[0]
        == 0
    )


def test_delta_rs_reads_written_types(extension_path, tmp_path):
    con = connect(extension_path)
    con.execute(f"ATTACH '{tmp_path / 'lake'}' AS w (TYPE delta_classic, READ_ONLY false)")
    con.execute(
        "CREATE TABLE w.main.types (id INTEGER, b BOOLEAN, i INTEGER, bi BIGINT, d DOUBLE, dec DECIMAL(18, 3), "
        "s VARCHAR, dt DATE, ts TIMESTAMP, tstz TIMESTAMPTZ, l INTEGER[], st STRUCT(a INTEGER, b VARCHAR))"
    )
    con.execute(
        "CREATE TABLE src AS SELECT range::INTEGER AS id, range % 2 = 0 AS b, range::INTEGER * 7 AS i, "
        "range * 1000000007 AS bi, range / 7 AS d, (range / 8)::DECIMAL(18, 3) AS dec, 'ünï ' || range AS s, "
        "DATE '2024-01-01' + range::INTEGER AS dt, TIMESTAMP '2024-01-01' + INTERVAL (range) SECOND AS ts, "
        "TIMESTAMPTZ '2024-01-01 00:00:00+00' + INTERVAL (range) MINUTE AS tstz, [range::INTEGER, NULL] AS l, "
        "{'a': range::INTEGER, 'b': 'x' || range} AS st FROM range(10000)"
    )
    con.execute("INSERT INTO src VALUES (-1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)")
    con.execute("INSERT INTO w.main.types SELECT * FROM src")

    arrow = deltalake.DeltaTable(str(tmp_path / "lake" / "types")).to_pyarrow_table()
    con.register("from_delta_rs", arrow)
    assert arrow.num_rows == 10001
    assert same_rows(con, "from_delta_rs", "src")
    assert same_rows(con, "w.main.types", "src")


def test_delta_rs_reads_written_partitions(extension_path, tmp_path):
    con = connect(extension_path)
    con.execute(f"ATTACH '{tmp_path / 'lake'}' AS w (TYPE delta_classic, READ_ONLY false)")
    con.execute("CREATE TABLE w.main.parts (id INTEGER, part VARCHAR) PARTITIONED BY (part)")
    con.execute(
        "INSERT INTO w.main.parts SELECT range, CASE range % 6 WHEN 0 THEN NULL WHEN 1 THEN 'a' "
        "WHEN 2 THEN 'with space' WHEN 3 THEN 'a/b' WHEN 4 THEN 'x=y' ELSE 'ünï' END FROM range(6000)"
    )
    arrow = deltalake.DeltaTable(str(tmp_path / "lake" / "parts")).to_pyarrow_table()
    con.register("from_delta_rs", arrow)
    assert same_rows(con, "from_delta_rs", "w.main.parts")
    counts = dict(con.execute("SELECT part, count(*) FROM from_delta_rs GROUP BY part").fetchall())
    assert counts == {None: 1000, "a": 1000, "with space": 1000, "a/b": 1000, "x=y": 1000, "ünï": 1000}


def test_append_to_delta_rs_table(extension_path, tmp_path):
    """delta-rs creates the table, delta_classic appends, both read the union."""
    path = tmp_path / "lake" / "sales" / "orders"
    first = pa.table({"id": pa.array(range(500), pa.int64()), "region": pa.array(["eu"] * 500)})
    deltalake.write_deltalake(str(path), first)

    con = connect(extension_path)
    con.execute(f"ATTACH '{tmp_path / 'lake'}' AS w (TYPE delta_classic, READ_ONLY false)")
    assert con.execute("SELECT count(*) FROM w.sales.orders").fetchone()[0] == 500
    con.execute("INSERT INTO w.sales.orders SELECT 500 + range, 'us' FROM range(300)")

    table = deltalake.DeltaTable(str(path))
    assert table.version() == 1
    arrow = table.to_pyarrow_table()
    con.register("from_delta_rs", arrow)
    assert same_rows(con, "from_delta_rs", "w.sales.orders")
    assert con.execute("SELECT count(*), count(DISTINCT id), sum(id) FROM from_delta_rs").fetchone() == (
        800,
        800,
        sum(range(800)),
    )

    # delta-rs appends again on top of delta_classic's commit
    deltalake.write_deltalake(str(path), pa.table({"id": pa.array([800], pa.int64()), "region": ["eu"]}), mode="append")
    con.execute("DETACH w")
    con.execute(f"ATTACH '{tmp_path / 'lake'}' AS r (TYPE delta_classic)")
    assert con.execute("SELECT count(*), max(id) FROM r.sales.orders").fetchone() == (801, 800)
