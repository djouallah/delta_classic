"""Test multi-schema mode where child dirs are schemas containing tables."""


def test_discover_schemas(conn):
    conn.execute("ATTACH 'test/data/multi_schema' AS mdb (TYPE delta_classic)")
    schemas = conn.execute(
        "SELECT schema_name FROM duckdb_schemas() "
        "WHERE database_name = 'mdb' AND schema_name NOT IN ('information_schema', 'main') "
        "ORDER BY schema_name"
    ).fetchall()
    assert [r[0] for r in schemas] == ["schema1", "schema2"]
    conn.execute("DETACH mdb")


def test_tables_in_schemas(conn):
    conn.execute("ATTACH 'test/data/multi_schema' AS mdb (TYPE delta_classic)")
    tables_s1 = conn.execute(
        "SELECT table_name FROM duckdb_tables() "
        "WHERE database_name = 'mdb' AND schema_name = 'schema1' ORDER BY table_name"
    ).fetchall()
    assert [r[0] for r in tables_s1] == ["table_x", "table_y"]

    tables_s2 = conn.execute(
        "SELECT table_name FROM duckdb_tables() "
        "WHERE database_name = 'mdb' AND schema_name = 'schema2'"
    ).fetchall()
    assert [r[0] for r in tables_s2] == [("table_z",)[0]]
    conn.execute("DETACH mdb")


def test_query_across_schemas(conn):
    conn.execute("ATTACH 'test/data/multi_schema' AS mdb (TYPE delta_classic)")

    rows_x = conn.execute("SELECT id, region, amount FROM mdb.schema1.table_x ORDER BY id").fetchall()
    assert len(rows_x) == 5
    assert rows_x[0] == (1, "east", 100)

    rows_y = conn.execute("SELECT key, val FROM mdb.schema1.table_y ORDER BY key").fetchall()
    assert rows_y == [("a", 1), ("b", 2)]

    rows_z = conn.execute("SELECT id, label FROM mdb.schema2.table_z ORDER BY id").fetchall()
    assert rows_z == [(10, "foo"), (20, "bar"), (30, "baz")]

    conn.execute("DETACH mdb")


def test_aggregation(conn):
    conn.execute("ATTACH 'test/data/multi_schema' AS mdb (TYPE delta_classic)")
    total = conn.execute("SELECT SUM(amount) FROM mdb.schema1.table_x").fetchone()[0]
    assert total == 1000
    conn.execute("DETACH mdb")
