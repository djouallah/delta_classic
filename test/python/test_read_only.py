"""Test that delta_classic databases reject all write operations."""

import pytest
import duckdb


def test_create_table_fails(conn):
    conn.execute("ATTACH 'test/data/single_schema' AS rodb (TYPE delta_classic)")
    with pytest.raises(duckdb.BinderException, match="read-only"):
        conn.execute("CREATE TABLE rodb.main.new_table (id INTEGER)")
    conn.execute("DETACH rodb")


def test_drop_table_fails(conn):
    conn.execute("ATTACH 'test/data/single_schema' AS rodb (TYPE delta_classic)")
    with pytest.raises(duckdb.BinderException, match="read-only"):
        conn.execute("DROP TABLE rodb.main.table_a")
    conn.execute("DETACH rodb")


def test_create_schema_fails(conn):
    conn.execute("ATTACH 'test/data/single_schema' AS rodb (TYPE delta_classic)")
    with pytest.raises(duckdb.BinderException, match="read-only"):
        conn.execute("CREATE SCHEMA rodb.new_schema")
    conn.execute("DETACH rodb")


def test_insert_fails(conn):
    conn.execute("ATTACH 'test/data/single_schema' AS rodb (TYPE delta_classic)")
    # Query first to trigger internal attach
    conn.execute("SELECT COUNT(*) FROM rodb.main.table_a")
    with pytest.raises(Exception, match="read-only"):
        conn.execute("INSERT INTO rodb.main.table_a VALUES (99, 'test', 99.0)")
    conn.execute("DETACH rodb")


def test_update_fails(conn):
    conn.execute("ATTACH 'test/data/single_schema' AS rodb (TYPE delta_classic)")
    conn.execute("SELECT COUNT(*) FROM rodb.main.table_a")
    with pytest.raises(Exception):
        conn.execute("UPDATE rodb.main.table_a SET name = 'test' WHERE id = 1")
    conn.execute("DETACH rodb")


def test_delete_fails(conn):
    conn.execute("ATTACH 'test/data/single_schema' AS rodb (TYPE delta_classic)")
    conn.execute("SELECT COUNT(*) FROM rodb.main.table_a")
    with pytest.raises(Exception):
        conn.execute("DELETE FROM rodb.main.table_a WHERE id = 1")
    conn.execute("DETACH rodb")


def test_drop_schema_fails(conn):
    conn.execute("ATTACH 'test/data/single_schema' AS rodb (TYPE delta_classic)")
    with pytest.raises(Exception, match="read-only"):
        conn.execute("DROP SCHEMA rodb.main")
    conn.execute("DETACH rodb")


def test_alter_table_fails(conn):
    conn.execute("ATTACH 'test/data/single_schema' AS rodb (TYPE delta_classic)")
    conn.execute("SELECT COUNT(*) FROM rodb.main.table_a")
    with pytest.raises(Exception):
        conn.execute("ALTER TABLE rodb.main.table_a ADD COLUMN extra INTEGER")
    conn.execute("DETACH rodb")


def test_create_view_fails(conn):
    conn.execute("ATTACH 'test/data/single_schema' AS rodb (TYPE delta_classic)")
    with pytest.raises(Exception, match="read-only"):
        conn.execute("CREATE VIEW rodb.main.my_view AS SELECT 1")
    conn.execute("DETACH rodb")


def test_create_index_fails(conn):
    conn.execute("ATTACH 'test/data/single_schema' AS rodb (TYPE delta_classic)")
    conn.execute("SELECT COUNT(*) FROM rodb.main.table_a")
    with pytest.raises(Exception):
        conn.execute("CREATE INDEX idx ON rodb.main.table_a(id)")
    conn.execute("DETACH rodb")


def test_insert_select_fails(conn):
    conn.execute("ATTACH 'test/data/single_schema' AS rodb (TYPE delta_classic)")
    conn.execute("SELECT COUNT(*) FROM rodb.main.table_a")
    with pytest.raises(Exception):
        conn.execute("INSERT INTO rodb.main.table_a SELECT * FROM rodb.main.table_a")
    conn.execute("DETACH rodb")


def test_truncate_fails(conn):
    conn.execute("ATTACH 'test/data/single_schema' AS rodb (TYPE delta_classic)")
    conn.execute("SELECT COUNT(*) FROM rodb.main.table_a")
    with pytest.raises(Exception):
        conn.execute("TRUNCATE TABLE rodb.main.table_a")
    conn.execute("DETACH rodb")
