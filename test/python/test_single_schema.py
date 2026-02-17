"""Test single-schema mode where delta tables are direct children."""


def test_attach_and_list_tables(conn):
    conn.execute("ATTACH 'test/data/single_schema' AS sdb (TYPE delta_classic)")
    tables = conn.execute(
        "SELECT table_name FROM duckdb_tables() WHERE database_name = 'sdb' ORDER BY table_name"
    ).fetchall()
    assert [r[0] for r in tables] == ["table_a", "table_b"]
    conn.execute("DETACH sdb")


def test_query_table_a(conn):
    conn.execute("ATTACH 'test/data/single_schema' AS sdb (TYPE delta_classic)")
    rows = conn.execute("SELECT id, name, value FROM sdb.main.table_a ORDER BY id").fetchall()
    assert rows == [(1, "alice", 10.0), (2, "bob", 20.0), (3, "charlie", 30.0)]
    conn.execute("DETACH sdb")


def test_query_table_b(conn):
    conn.execute("ATTACH 'test/data/single_schema' AS sdb (TYPE delta_classic)")
    rows = conn.execute("SELECT id, category FROM sdb.main.table_b ORDER BY id").fetchall()
    assert rows == [(100, "x"), (200, "y")]
    conn.execute("DETACH sdb")


def test_use_database_unqualified(conn):
    conn.execute("ATTACH 'test/data/single_schema' AS sdb (TYPE delta_classic)")
    conn.execute("USE sdb")
    count = conn.execute("SELECT COUNT(*) FROM table_a").fetchone()[0]
    assert count == 3
    conn.execute("USE memory")
    conn.execute("DETACH sdb")
