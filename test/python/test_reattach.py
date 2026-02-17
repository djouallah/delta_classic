"""Test detach/re-attach and ATTACH OR REPLACE behavior."""


def test_detach_and_reattach_same_name(conn):
    """Detach then re-attach with same name but different path."""
    conn.execute("ATTACH 'test/data/single_schema' AS mydb (TYPE delta_classic)")
    count = conn.execute("SELECT COUNT(*) FROM mydb.main.table_a").fetchone()[0]
    assert count == 3

    conn.execute("DETACH mydb")

    # Re-attach same name, different path
    conn.execute("ATTACH 'test/data/multi_schema' AS mydb (TYPE delta_classic)")
    count = conn.execute("SELECT COUNT(*) FROM mydb.schema1.table_x").fetchone()[0]
    assert count == 5

    conn.execute("DETACH mydb")


def test_attach_or_replace(conn):
    """ATTACH OR REPLACE should swap the database cleanly."""
    conn.execute("ATTACH 'test/data/single_schema' AS rdb (TYPE delta_classic)")
    count = conn.execute("SELECT COUNT(*) FROM rdb.main.table_a").fetchone()[0]
    assert count == 3

    # Replace with different path
    conn.execute("ATTACH OR REPLACE 'test/data/multi_schema' AS rdb (TYPE delta_classic)")
    count = conn.execute("SELECT COUNT(*) FROM rdb.schema1.table_x").fetchone()[0]
    assert count == 5

    conn.execute("DETACH rdb")

    # No orphaned internal databases
    orphaned = conn.execute(
        "SELECT COUNT(*) FROM duckdb_databases() WHERE database_name LIKE '__dc_%'"
    ).fetchone()[0]
    assert orphaned == 0
