"""Test error handling for invalid paths, missing tables, and missing schemas."""

import pytest
import duckdb


def test_nonexistent_path(conn):
    with pytest.raises(Exception):
        conn.execute("ATTACH 'test/data/nonexistent_path' AS bad1 (TYPE delta_classic)")


def test_missing_table(conn):
    conn.execute("ATTACH 'test/data/single_schema' AS edb (TYPE delta_classic)")
    with pytest.raises(Exception):
        conn.execute("SELECT * FROM edb.main.no_such_table")
    conn.execute("DETACH edb")


def test_missing_schema(conn):
    conn.execute("ATTACH 'test/data/single_schema' AS edb (TYPE delta_classic)")
    with pytest.raises(Exception):
        conn.execute("SELECT * FROM edb.no_such_schema.table_a")
    conn.execute("DETACH edb")


def test_missing_schema_multi(conn):
    conn.execute("ATTACH 'test/data/multi_schema' AS mdb (TYPE delta_classic)")
    with pytest.raises(Exception):
        conn.execute("SELECT * FROM mdb.nonexistent_schema.table_x")
    conn.execute("DETACH mdb")
