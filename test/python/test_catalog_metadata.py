"""Test catalog metadata: duckdb_databases, duckdb_tables, duckdb_schemas, DESCRIBE."""

import pytest


def test_database_appears_in_duckdb_databases(conn):
    conn.execute("ATTACH 'test/data/single_schema' AS sdb (TYPE delta_classic)")
    count = conn.execute(
        "SELECT COUNT(*) FROM duckdb_databases() WHERE database_name = 'sdb'"
    ).fetchone()[0]
    assert count == 1
    conn.execute("DETACH sdb")


def test_tables_listed_in_duckdb_tables(conn):
    conn.execute("ATTACH 'test/data/single_schema' AS sdb (TYPE delta_classic)")
    tables = conn.execute(
        "SELECT table_name FROM duckdb_tables() WHERE database_name = 'sdb' ORDER BY table_name"
    ).fetchall()
    assert [r[0] for r in tables] == ["table_a", "table_b"]
    conn.execute("DETACH sdb")


def test_schema_listed_in_duckdb_schemas(conn):
    conn.execute("ATTACH 'test/data/single_schema' AS sdb (TYPE delta_classic)")
    schemas = conn.execute(
        "SELECT schema_name FROM duckdb_schemas() WHERE database_name = 'sdb' AND schema_name = 'main'"
    ).fetchall()
    assert len(schemas) == 1
    conn.execute("DETACH sdb")


@pytest.mark.skip(reason="DESCRIBE re-binds the table, internal DB not visible across statements yet")
def test_describe_single_schema_table(conn):
    conn.execute("ATTACH 'test/data/single_schema' AS sdb (TYPE delta_classic)")
    conn.execute("SELECT COUNT(*) FROM sdb.main.table_a")
    cols = conn.execute(
        "SELECT column_name, data_type FROM (DESCRIBE sdb.main.table_a) ORDER BY column_name"
    ).fetchall()
    col_names = [r[0] for r in cols]
    assert "id" in col_names
    assert "name" in col_names
    assert "value" in col_names
    conn.execute("DETACH sdb")


def test_multi_schema_tables_in_correct_schemas(conn):
    conn.execute("ATTACH 'test/data/multi_schema' AS mdb (TYPE delta_classic)")
    rows = conn.execute(
        "SELECT schema_name, table_name FROM duckdb_tables() "
        "WHERE database_name = 'mdb' AND schema_name IN ('schema1', 'schema2') "
        "ORDER BY schema_name, table_name"
    ).fetchall()
    assert rows == [("schema1", "table_x"), ("schema1", "table_y"), ("schema2", "table_z")]
    conn.execute("DETACH mdb")


def test_pin_snapshot_option_accepted(conn):
    """PIN_SNAPSHOT should be accepted without error."""
    conn.execute("ATTACH 'test/data/single_schema' AS pindb (TYPE delta_classic, PIN_SNAPSHOT)")
    count = conn.execute("SELECT COUNT(*) FROM pindb.main.table_a").fetchone()[0]
    assert count == 3
    conn.execute("DETACH pindb")
