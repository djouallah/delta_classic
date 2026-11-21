# Delta Classic - DuckDB Extension

A minimal DuckDB extension that lets you attach a **directory of Delta tables** as a single database. Works with any filesystem DuckDB supports: local, S3 (and S3-compatible stores like MinIO), GCS, Azure (ABFSS) and OneLake.

## Installation

Requires DuckDB v2.0.1.

```sql
INSTALL delta_classic FROM community;
LOAD delta_classic;

ATTACH '/data/lake' AS lake (TYPE delta_classic);
SELECT * FROM lake.sales.orders;
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
3. Internally calls `ATTACH ... (TYPE DELTA)` for each table when you first query it
4. Exposes them all through a single virtual catalog

There is zero reimplementation of Delta reading — everything delegates to the existing [Delta extension](https://github.com/duckdb/duckdb-delta).

## Schema Discovery

The extension auto-detects the directory structure. The same rules apply on every filesystem:

```sql
ATTACH '/data/lake'       AS db (TYPE delta_classic);   -- local
ATTACH 's3://bucket/lake' AS db (TYPE delta_classic);   -- S3, MinIO
ATTACH 'gs://bucket/lake' AS db (TYPE delta_classic);   -- GCS
ATTACH 'abfss://container@account.dfs.core.windows.net/lake' AS db (TYPE delta_classic);   -- Azure
```

**Multi-schema** — when the path contains subdirectories of tables:
```
lake/
├── sales/            → schema "sales"
│   ├── orders/       → table (has _delta_log)
│   └── customers/    → table (has _delta_log)
└── hr/               → schema "hr"
    └── people/       → table
```
```sql
ATTACH '/data/lake' AS db (TYPE delta_classic);
SELECT * FROM db.sales.orders;
```

**Single-schema** — when the path directly contains Delta tables:
```
sales/
├── orders/_delta_log/
└── customers/_delta_log/
```
```sql
ATTACH '/data/lake/sales' AS db (TYPE delta_classic);
SELECT * FROM db.main.orders;
```

**Nested schemas** — a directory inside a schema that isn't a table is a nested schema, at any depth:
```
lake/
└── sales/            → schema "sales"
    ├── targets/      → table sales.targets
    └── eu/           → schema "sales.eu"
        └── orders/   → table sales.eu.orders
```
```sql
ATTACH '/data/lake' AS db (TYPE delta_classic);
SELECT * FROM db.sales.eu.orders;
USE db.sales.eu;
```

**Single table** — when the path is itself a Delta table:
```sql
ATTACH '/data/lake/sales/orders' AS db (TYPE delta_classic);
SELECT * FROM db.main.orders;
```

Directories starting with `.` or `_` are skipped, and nothing below a table (its partition directories) is ever a schema.

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
SELECT * FROM db.sales.orders AT (VERSION => 42);
SELECT * FROM db.sales.orders AT (TIMESTAMP => TIMESTAMPTZ '2026-10-01 00:00:00+00');
```

`AT` overrides an attached `TIMESTAMP`.

## Writing

Databases are read-only unless attached with `READ_ONLY false`. `CREATE SCHEMA`, `CREATE TABLE`, `CREATE TABLE AS SELECT` and `INSERT` then work, and the Delta extension writes the tables:

```sql
ATTACH '/data/lake' AS db (TYPE delta_classic, READ_ONLY false);

CREATE SCHEMA db.sales;                                   -- lake/sales
CREATE SCHEMA db.sales.eu;                                -- lake/sales/eu
CREATE TABLE db.sales.returns (id INTEGER, region VARCHAR) PARTITIONED BY (region);   -- lake/sales/returns
INSERT INTO db.sales.returns VALUES (1, 'eu'), (2, 'us');
CREATE TABLE db.sales.eu.by_region AS                     -- lake/sales/eu/by_region
    SELECT region, COUNT(*) AS n FROM db.sales.returns GROUP BY region;
```

- `CREATE TABLE` writes a new Delta table in the schema's directory. It takes `PARTITIONED BY`, Delta table properties in `WITH ('delta.enableDeletionVectors' = 'true')` and `IF NOT EXISTS`, like the Delta extension. `CREATE TABLE AS SELECT` takes the same options, and commits the table with its rows as version 0.
- `CREATE SCHEMA` creates the schema's directory, nested schemas included. S3 and GCS have no directories, so an empty schema there lasts only until DETACH: create a table in it to keep it.
- In a new directory, the first `CREATE` decides the layout: a table makes it a single schema (`main`), a schema makes it a multi-schema database. In a multi-schema database, tables go in a schema. A path that is itself a Delta table can't hold new tables or schemas.
- `TIMESTAMP` and `PIN_SNAPSHOT` can't be combined with `READ_ONLY false`.
- Not supported, as in the Delta extension: `CREATE OR REPLACE`, `UPDATE`, `DELETE`, `MERGE`, `DROP`, `DROP SCHEMA`, `ALTER`, `RETURNING`, `ON CONFLICT`.
- Concurrent writers to one table, from any process or machine, race for its next Delta version: one wins, the others fail with an error and leave nothing behind, so retry them.
- Each table commits on its own: a transaction that writes several tables is not atomic across them. `CREATE TABLE`, `CREATE TABLE AS SELECT` and `CREATE SCHEMA` are committed immediately, and `ROLLBACK` doesn't undo them. A table written in a transaction can't be read again in that transaction.

## Limitations

- Not available in DuckDB-WASM, because the Delta extension has no WASM build.
- Tables and schemas are listed once, on first use. Attach again to see new ones.
- `duckdb_columns()` and `information_schema.columns` only show the columns of tables already queried in the current transaction (listing every column would read every Delta log). `DESCRIBE` always works.

## Microsoft Fabric / OneLake

OneLake is just another filesystem, but delta_classic also knows Fabric's layout, so a whole workspace attaches as one database: `<workspace>.<lakehouse>.<schema>.<table>`.

```
<workspace>/
├── sales.Lakehouse/Tables/dbo/orders   → ws.sales.dbo.orders
├── sales.Lakehouse/Tables/gold/daily   → ws.sales.gold.daily
├── hr.Lakehouse/Tables/dbo/people      → ws.hr.dbo.people
└── wh.Warehouse/...                    → left out (not a lakehouse)
```
```sql
CREATE SECRET (TYPE azure, PROVIDER credential_chain, CHAIN 'cli', ACCOUNT_NAME 'onelake');

-- A workspace: each lakehouse is a schema, holding its own schemas
ATTACH 'abfss://<workspace>@onelake.dfs.fabric.microsoft.com/' AS ws (TYPE delta_classic);
SELECT * FROM ws.sales.dbo.orders o JOIN ws.hr.dbo.people p ON o.person_id = p.id;
SELECT * FROM ws."<lakehouse id>".dbo.orders;   -- by id works too

-- A lakehouse on its own: <db>.<schema>.<table>
ATTACH 'abfss://<workspace>@onelake.dfs.fabric.microsoft.com/sales.Lakehouse' AS lh (TYPE delta_classic);
SELECT * FROM lh.dbo.orders;
```

Writing works as anywhere else, into lakehouses that exist in Fabric:

```sql
ATTACH 'abfss://<workspace>@onelake.dfs.fabric.microsoft.com/sales.Lakehouse' AS lh (TYPE delta_classic, READ_ONLY false);

CREATE SCHEMA lh.staging;                  -- Tables/staging
CREATE TABLE lh.staging.daily AS           -- Tables/staging/daily
    SELECT day, SUM(amount) AS amount FROM lh.dbo.orders GROUP BY day;
```

- A lakehouse is a `<name>.Lakehouse` directory, or a directory holding `Tables` and `Files` (a lakehouse addressed by id: `abfss://<workspace id>@onelake.dfs.fabric.microsoft.com/<lakehouse id>`).
- Only lakehouses with schemas are supported. Using a lakehouse without schemas (its tables directly in `Tables`) fails with an error that says to attach its `Tables` directory instead, which reads it as `<db>.main.<table>`. In a lakehouse with schemas, stray tables directly in `Tables` are left out. Lakehouse schemas don't nest.
- Lakehouses can't be created from DuckDB: create them in Fabric. `CREATE SCHEMA` creates a schema inside a lakehouse.
- In a workspace, a lakehouse is found by name or id the first time it is looked up. Listing a OneLake workspace fails with the Azure SDK the azure extension uses today ([duckdb-azure#226](https://github.com/duckdb/duckdb-azure/issues/226)), so until that is fixed `duckdb_schemas()` shows only the lakehouses already looked up, and a single OneLake table can't be attached on its own.
- Write in autocommit mode: on OneLake, an `INSERT` inside an explicit `BEGIN ... COMMIT` can fail with an internal error in the Delta and azure extensions, with or without delta_classic.
- In a Fabric notebook, upgrade DuckDB first: `!pip install duckdb --upgrade`, then `notebookutils.session.restartPython()`.

## Why "Classic"?

The name is a tongue-in-cheek reference to what Delta has become. Delta Lake started as a beautifully simple idea — Parquet files plus a transaction log on storage. But with [catalog-managed commits](https://learn.microsoft.com/en-us/azure/databricks/delta/catalog-managed-commits), Unity Catalog has taken over as the transaction coordinator itself. Commits are no longer just appended to `_delta_log` by the compute engine — they're validated, tracked, and ordered server-side by UC. The transaction log on disk is no longer the source of truth. Delta, in practice, has become a catalog-managed table format.

To be fair, catalog-managed commits aren't a bad thing — they're essential for complex scenarios like multiple concurrent writers, multi-table transactions, and centralized governance at scale. If you need that, you need that.

But many workloads are simpler: read-heavy analytics, single-writer pipelines, local exploration. For those, a catalog server is overhead you don't need. **Delta Classic** is a nod to that original idea: your tables are just directories with a `_delta_log`, and that log *is* the truth. No catalog service coordinating your commits. Just files on storage, the way Delta started.

## Why Storage-Based Delta Management?

Using plain storage (directories of Delta tables) instead of a metastore like Unity Catalog has real advantages:

- **No catalog dependency** — you don't need Unity Catalog, Hive Metastore, or any external service to organize your tables
- **Works everywhere** — local filesystem, S3, ABFSS, GCS, OneLake. Just point at a directory
- **Simple and portable** — your data layout *is* your catalog. Copy a folder, and you've copied a database

This extension just makes that pattern first-class in DuckDB.

## Why an Extension?

Honestly, AI is becoming good enough to generate full extension out of thin air

Requires DuckDB v2.0.1.
