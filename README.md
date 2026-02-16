# Delta Classic - DuckDB Extension

A minimal DuckDB extension that lets you attach a **directory of Delta tables** as a single database.

## The Problem

Today, if you have a directory with multiple Delta tables, you need to attach each one individually:

```python
for tbl in tpch_tables:
    conn.sql(f"ATTACH '{base_path}/{tbl}' AS {tbl} (TYPE DELTA, PIN_SNAPSHOT)")
```

This creates a separate database per table. Annoying.

## The Solution

```sql
LOAD delta;
LOAD delta_classic;

ATTACH 'abfss://tpch@onelake.dfs.fabric.microsoft.com/raw.Lakehouse/Tables' AS tpch (TYPE delta_classic, PIN_SNAPSHOT);

-- All tables are now available
SELECT * FROM tpch.CH0030.orders;
SHOW ALL TABLES;
```

That's it. One command, all your Delta tables.

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
SELECT * FROM db.CH0030.orders;
```

Works with any path DuckDB supports: local, S3, ABFSS, GCS.

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

Honestly, this should probably be a PR to the [Delta extension](https://github.com/duckdb/duckdb-delta) itself — it's a natural fit. But writing a standalone extension has a much lower entry bar, and this was the fastest way to get it working. If it proves useful, maybe it'll find its way upstream one day.

## Usage with Microsoft Fabric / OneLake

```python
import duckdb

conn = duckdb.connect()
conn.sql("INSTALL delta_classic FROM community; LOAD delta_classic")
conn.sql(f"""
    ATTACH 'abfss://{ws}@onelake.dfs.fabric.microsoft.com/{lh}.Lakehouse/Tables/{schema}'
    AS db (TYPE delta_classic, PIN_SNAPSHOT);
    USE db;
""")
```

## Building

```bash
make release
```

Requires DuckDB v1.4.4.
