"""
Generate Delta Lake test fixtures for delta_classic extension tests.

Usage:
    pip install deltalake pyarrow
    python test/generate_test_data.py

Creates test/data/ with Delta tables in two layouts:
  - single_schema/  (tables directly under root)
  - multi_schema/   (schema dirs containing table dirs)
"""
import os
import shutil
import pyarrow as pa
from deltalake import write_deltalake

BASE = os.path.join(os.path.dirname(__file__), "data")


def clean():
    if os.path.exists(BASE):
        shutil.rmtree(BASE)


def make_table(path, table):
    """Write a pyarrow table as a Delta table at the given path."""
    write_deltalake(path, table, mode="overwrite")


def generate_single_schema():
    """
    Layout:
      test/data/single_schema/
        table_a/_delta_log/...
        table_b/_delta_log/...
    """
    root = os.path.join(BASE, "single_schema")

    # table_a: simple integers and strings
    t_a = pa.table({
        "id": pa.array([1, 2, 3], type=pa.int64()),
        "name": pa.array(["alice", "bob", "charlie"], type=pa.string()),
        "value": pa.array([10.0, 20.0, 30.0], type=pa.float64()),
    })
    make_table(os.path.join(root, "table_a"), t_a)

    # table_b: different schema
    t_b = pa.table({
        "id": pa.array([100, 200], type=pa.int64()),
        "category": pa.array(["x", "y"], type=pa.string()),
    })
    make_table(os.path.join(root, "table_b"), t_b)


def generate_multi_schema():
    """
    Layout:
      test/data/multi_schema/
        schema1/
          table_x/_delta_log/...
          table_y/_delta_log/...
        schema2/
          table_z/_delta_log/...
    """
    root = os.path.join(BASE, "multi_schema")

    # schema1/table_x
    t_x = pa.table({
        "id": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
        "region": pa.array(["east", "west", "east", "west", "east"], type=pa.string()),
        "amount": pa.array([100, 200, 150, 250, 300], type=pa.int64()),
    })
    make_table(os.path.join(root, "schema1", "table_x"), t_x)

    # schema1/table_y
    t_y = pa.table({
        "key": pa.array(["a", "b"], type=pa.string()),
        "val": pa.array([1, 2], type=pa.int64()),
    })
    make_table(os.path.join(root, "schema1", "table_y"), t_y)

    # schema2/table_z
    t_z = pa.table({
        "id": pa.array([10, 20, 30], type=pa.int64()),
        "label": pa.array(["foo", "bar", "baz"], type=pa.string()),
    })
    make_table(os.path.join(root, "schema2", "table_z"), t_z)


if __name__ == "__main__":
    clean()
    generate_single_schema()
    generate_multi_schema()
    print(f"Test data generated in {BASE}/")
