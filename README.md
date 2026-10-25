# Delta Classic - DuckDB Extension

A minimal DuckDB extension that lets you attach a **directory of Delta tables** as a single database. Works with any filesystem DuckDB supports: local, S3, ABFSS, GCS.

## Installation

Requires DuckDB v2.0.0. Fabric notebook uses an old version, so you need to upgrade

```python
!pip install duckdb --upgrade
notebookutils.session.restartPython()
```


```python
import duckdb

conn = duckdb.connect()
conn.sql("install delta_classic FROM community ")
conn.sql(f"""
    ATTACH 'abfss://{ws}@onelake.dfs.fabric.microsoft.com/{lh}.Lakehouse/Tables/{schema}'
    AS db (TYPE delta_classic, PIN_SNAPSHOT);
    USE db;
""")
```

## The Problem

Today, if you have a directory with multiple Delta tables, you need to attach each one individually:

```python
for tbl in tpch_tables:
    conn.sql(f"ATTACH '{base_path}/{tbl}' AS {tbl} (TYPE DELTA, PIN_SNAPSHOT)")
```

This creates a separate database per table. Annoying.

## How It Works

This extension does the absolute minimum:

1. Lists subdirectories at the given path
2. Checks which ones have a `_delta_log` folder (i.e., are actual Delta tables)
3. Internally calls `ATTACH ... (TYPE DELTA, PIN_SNAPSHOT)` for each table when you first query it
4. Exposes them all through a single virtual catalog

There is zero reimplementation of Delta reading — everything delegates to the existing [Delta extension](https://github.com/duckdb/duckdb-delta).

## Schema Discovery

The extension auto-detects the directory structure:

**Multi-schema** — when the path contains subdirectories of tables:
```
Tables/
├── CH0030/           → schema "CH0030"
│   ├── orders/       → table (has _delta_log)
│   └── customers/    → table (has _delta_log)
└── CH0060/           → schema "CH0060"
    └── orders/       → table
```
```sql
ATTACH '.../Tables' AS db (TYPE delta_classic);
SELECT * FROM db.CH0030.orders;
```

**Single-schema** — when the path directly contains Delta tables:
```
CH0030/
├── orders/_delta_log/
└── customers/_delta_log/
```
```sql
ATTACH '.../CH0030' AS db (TYPE delta_classic);
SELECT * FROM db.main.orders;
```

**Single table** — when the path is itself a Delta table:
```sql
ATTACH '.../CH0030/orders' AS db (TYPE delta_classic);
SELECT * FROM db.main.orders;
```

Directories starting with `.` or `_` are skipped.

Works with any path DuckDB supports: local, S3 (including S3-compatible stores like MinIO), ABFSS, GCS.

## Options

```sql
ATTACH 's3://bucket/lake' AS db (
    TYPE delta_classic,
    PIN_SNAPSHOT,                          -- read each table's snapshot once, then reuse it
    TIMESTAMP '2026-10-01 00:00:00+00',    -- read every table as of this point in time
    PUSHDOWN_FILTERS 'all',                -- none | all | constant_only | dynamic_only
    PUSHDOWN_PARTITION_INFO true
);
```

All options are passed to the Delta extension for each table. `VERSION` is not supported, because Delta versions are per table: use `TIMESTAMP`, or `AT (VERSION => ...)` on a single table.

## Time Travel

```sql
SELECT * FROM db.main.orders AT (VERSION => 42);
SELECT * FROM db.main.orders AT (TIMESTAMP => TIMESTAMPTZ '2026-10-01 00:00:00+00');
```

`AT` overrides an attached `TIMESTAMP`.

## Writing

Databases are read-only unless attached with `READ_ONLY false`. `CREATE TABLE` and `INSERT` are then passed to the Delta extension:

```sql
ATTACH 's3://bucket/lake' AS db (TYPE delta_classic, READ_ONLY false);

CREATE TABLE db.sales.returns (id INTEGER, region VARCHAR) PARTITIONED BY (region);
INSERT INTO db.sales.returns VALUES (1, 'eu'), (2, 'us');
```

- `CREATE TABLE` writes a new Delta table in the schema's directory (`<path>/sales/returns`). It takes `PARTITIONED BY`, Delta table properties in `WITH ('delta.enableDeletionVectors' = 'true')` and `IF NOT EXISTS`, like the Delta extension.
- In a directory with one schema per subdirectory, tables go in a schema: a table directly in the attached directory would turn it into a single schema. A path that is itself a Delta table can't hold new tables.
- `TIMESTAMP` and `PIN_SNAPSHOT` can't be combined with `READ_ONLY false`.
- Not supported, as in the Delta extension: `CREATE TABLE AS SELECT`, `CREATE OR REPLACE`, `UPDATE`, `DELETE`, `MERGE`, `DROP`, `ALTER`, `CREATE SCHEMA`, `RETURNING`, `ON CONFLICT`.
- Each table commits on its own: a transaction that writes several tables is not atomic across them. `CREATE TABLE` is committed immediately and is not undone by `ROLLBACK`. A table written in a transaction can't be read again in that transaction.

## Limitations

- Not available in DuckDB-WASM, because the Delta extension has no WASM build.
- Tables and schemas are listed once, on first use. Attach again to see new ones.
- `duckdb_columns()` and `information_schema.columns` only show the columns of tables already queried in the current transaction (listing every column would read every Delta log). `DESCRIBE` always works.

## Why "Classic"?

The name is a tongue-in-cheek reference to what Delta has become. Delta Lake started as a beautifully simple idea — Parquet files plus a transaction log on storage. But with [catalog-managed commits](https://learn.microsoft.com/en-us/azure/databricks/delta/catalog-managed-commits), Unity Catalog has taken over as the transaction coordinator itself. Commits are no longer just appended to `_delta_log` by the compute engine — they're validated, tracked, and ordered server-side by UC. The transaction log on disk is no longer the source of truth. Delta, in practice, has become a catalog-managed table format.

To be fair, catalog-managed commits aren't a bad thing — they're essential for complex scenarios like multiple concurrent writers, multi-table transactions, and centralized governance at scale. If you need that, you need that.

But many workloads are simpler: read-heavy analytics, single-writer pipelines, local exploration. For those, a catalog server is overhead you don't need. **Delta Classic** is a nod to that original idea: your tables are just directories with a `_delta_log`, and that log *is* the truth. No catalog service coordinating your commits. Just files on storage, the way Delta started.

## Why Storage-Based Delta Management?

Using plain storage (directories of Delta tables) instead of a metastore like Unity Catalog has real advantages:

- **No catalog dependency** — you don't need Unity Catalog, Hive Metastore, or any external service to organize your tables
- **Works everywhere** — local filesystem, S3, ABFSS, GCS. Just point at a directory
- **Simple and portable** — your data layout *is* your catalog. Copy a folder, and you've copied a database

This extension just makes that pattern first-class in DuckDB.

## Why an Extension?

Honestly, AI is becoming good enough to generate full extension out of thin air

Requires DuckDB v2.0.0.
