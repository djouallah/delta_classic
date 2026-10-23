"""Test attaching multiple delta_classic databases simultaneously."""


def test_same_path_two_databases(conn):
    """Two databases pointing to the same path should both work."""
    conn.execute("ATTACH 'test/data/single_schema' AS same1 (TYPE delta_classic)")
    conn.execute("ATTACH 'test/data/single_schema' AS same2 (TYPE delta_classic)")

    count1 = conn.execute("SELECT COUNT(*) FROM same1.main.table_a").fetchone()[0]
    count2 = conn.execute("SELECT COUNT(*) FROM same2.main.table_a").fetchone()[0]
    assert count1 == 3
    assert count2 == 3

    # Detach one, the other should still work
    conn.execute("DETACH same1")
    count2_after = conn.execute("SELECT COUNT(*) FROM same2.main.table_b").fetchone()[0]
    assert count2_after == 2

    conn.execute("DETACH same2")


def test_different_paths(conn):
    """Two databases pointing to different paths should both work."""
    conn.execute("ATTACH 'test/data/single_schema' AS sdb (TYPE delta_classic)")
    conn.execute("ATTACH 'test/data/multi_schema' AS mdb (TYPE delta_classic)")

    count_a = conn.execute("SELECT COUNT(*) FROM sdb.main.table_a").fetchone()[0]
    count_x = conn.execute("SELECT COUNT(*) FROM mdb.schema1.table_x").fetchone()[0]
    assert count_a == 3
    assert count_x == 5

    conn.execute("DETACH sdb")
    conn.execute("DETACH mdb")


def test_cross_database_join(conn):
    """Joins across two delta_classic databases should work."""
    conn.execute("ATTACH 'test/data/single_schema' AS sdb (TYPE delta_classic)")
    conn.execute("ATTACH 'test/data/multi_schema' AS mdb (TYPE delta_classic)")

    count = conn.execute(
        "SELECT COUNT(*) FROM sdb.main.table_a AS a, mdb.schema1.table_x AS x WHERE a.id = x.id"
    ).fetchone()[0]
    assert count == 3

    conn.execute("DETACH sdb")
    conn.execute("DETACH mdb")


def test_cross_database_subquery(conn):
    """Subqueries across databases should work."""
    conn.execute("ATTACH 'test/data/single_schema' AS sdb (TYPE delta_classic)")
    conn.execute("ATTACH 'test/data/multi_schema' AS mdb (TYPE delta_classic)")

    total = conn.execute(
        "SELECT (SELECT COUNT(*) FROM sdb.main.table_a) + (SELECT COUNT(*) FROM mdb.schema1.table_x)"
    ).fetchone()[0]
    assert total == 8

    conn.execute("DETACH sdb")
    conn.execute("DETACH mdb")


def test_only_user_databases_visible(conn):
    """Querying tables must not add databases to the user-visible database list."""
    conn.execute("ATTACH 'test/data/single_schema' AS db1 (TYPE delta_classic)")
    conn.execute("ATTACH 'test/data/single_schema' AS db2 (TYPE delta_classic)")

    conn.execute("SELECT COUNT(*) FROM db1.main.table_a")
    conn.execute("SELECT COUNT(*) FROM db2.main.table_b")

    names = sorted(
        r[0]
        for r in conn.execute(
            "SELECT database_name FROM duckdb_databases() WHERE NOT internal AND database_name != 'memory'"
        ).fetchall()
    )
    assert names == ["db1", "db2"]

    conn.execute("DETACH db1")
    assert conn.execute("SELECT COUNT(*) FROM db2.main.table_a").fetchone()[0] == 3
    conn.execute("DETACH db2")
