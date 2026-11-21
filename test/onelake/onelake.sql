-- delta_classic against a real OneLake lakehouse, run by .github/workflows/OneLake.yml with the DuckDB CLI.
--
-- The workflow fills in ${LH} (abfss://<workspace id>@onelake.dfs.fabric.microsoft.com/<lakehouse id>), ${WS} (the
-- workspace root), ${LH_ID} (the lakehouse id) and ${SCHEMA} (a schema of its own per run). It runs a prelude first
-- that loads delta and delta_classic and creates the azure secret.
--
-- The CLI runs with -bail: the first statement that fails stops the run. An assertion is a SELECT that calls error()
-- when its condition is false. A statement that must be refused runs between .bail off and .bail on, and the
-- assertion after it checks that it left nothing behind.

.bail on

-- ============================================================
-- The lakehouse, by id: <db>.<schema>.<table>
-- ============================================================

ATTACH '${LH}' AS lh (TYPE delta_classic, READ_ONLY false);

CREATE SCHEMA lh.${SCHEMA};
CREATE SCHEMA IF NOT EXISTS lh.${SCHEMA};

CREATE TABLE lh.${SCHEMA}.orders (id INTEGER, person_id INTEGER, amount DOUBLE, day DATE) PARTITIONED BY (day);
INSERT INTO lh.${SCHEMA}.orders VALUES
    (1, 1, 10.0, DATE '2026-01-01'), (2, 2, 20.0, DATE '2026-01-01'), (3, 1, 30.0, DATE '2026-01-02');

CREATE TABLE lh.${SCHEMA}.people AS SELECT * FROM (VALUES (1, 'ann'), (2, 'bob')) t(id, name);

CREATE TABLE lh.${SCHEMA}.by_person PARTITIONED BY (name) AS
SELECT p.name, COUNT(*)::BIGINT AS n, SUM(o.amount) AS total
FROM lh.${SCHEMA}.orders o JOIN lh.${SCHEMA}.people p ON o.person_id = p.id GROUP BY p.name;

CREATE TABLE lh.${SCHEMA}.empty AS SELECT * FROM lh.${SCHEMA}.orders WHERE id < 0;

-- IF NOT EXISTS on an existing table skips the query
CREATE TABLE IF NOT EXISTS lh.${SCHEMA}.people AS SELECT 1 AS other;

INSERT INTO lh.${SCHEMA}.orders SELECT id + 10, id, 5.0, DATE '2026-01-03' FROM lh.${SCHEMA}.people;

SELECT CASE WHEN (SELECT COUNT(*) FROM lh.${SCHEMA}.orders) = 5 THEN 'ok: orders'
    ELSE error('orders: expected 5 rows') END;
SELECT CASE WHEN (SELECT COUNT(*) FROM lh.${SCHEMA}.orders AT (VERSION => 1)) = 3 THEN 'ok: time travel'
    ELSE error('orders AT VERSION 1: expected 3 rows') END;
SELECT CASE WHEN (SELECT string_agg(name || '=' || n || '/' || total, ',' ORDER BY name) FROM lh.${SCHEMA}.by_person)
    = 'ann=2/40.0,bob=1/20.0' THEN 'ok: by_person' ELSE error('by_person: wrong rows') END;
SELECT CASE WHEN (SELECT COUNT(*) FROM lh.${SCHEMA}.empty) = 0 THEN 'ok: empty'
    ELSE error('empty: expected no rows') END;
-- If IF NOT EXISTS had replaced people, it would have a column "other" and no "name"
SELECT CASE WHEN (SELECT count(name) FROM lh.${SCHEMA}.people) = 2 THEN 'ok: IF NOT EXISTS'
    ELSE error('people: IF NOT EXISTS replaced the table') END;

-- One transaction writes two tables; a rolled back INSERT is discarded
BEGIN;
INSERT INTO lh.${SCHEMA}.orders VALUES (20, 2, 1.0, DATE '2026-01-04');
INSERT INTO lh.${SCHEMA}.people VALUES (3, 'cy');
COMMIT;

BEGIN;
INSERT INTO lh.${SCHEMA}.people VALUES (4, 'dee');
ROLLBACK;

SELECT CASE WHEN (SELECT COUNT(*) FROM lh.${SCHEMA}.orders) = 6 AND (SELECT COUNT(*) FROM lh.${SCHEMA}.people) = 3
    THEN 'ok: transactions' ELSE error('transactions: expected 6 orders and 3 people') END;

SELECT CASE WHEN (SELECT COUNT(*) FROM duckdb_tables() WHERE database_name = 'lh' AND schema_name = '${SCHEMA}') = 4
    THEN 'ok: duckdb_tables' ELSE error('duckdb_tables: expected 4 tables in the schema') END;

-- Refused: a table outside a schema, a nested schema, CREATE OR REPLACE
.bail off
CREATE TABLE lh.main.outside (i INTEGER);
CREATE SCHEMA lh.${SCHEMA}.nested;
CREATE OR REPLACE TABLE lh.${SCHEMA}.people AS SELECT 1 AS i;
.bail on

SELECT CASE WHEN (SELECT COUNT(*) FROM duckdb_tables() WHERE database_name = 'lh' AND table_name = 'outside') = 0
    AND (SELECT COUNT(*) FROM duckdb_schemas() WHERE database_name = 'lh' AND schema_name = 'nested') = 0
    AND (SELECT COUNT(*) FROM lh.${SCHEMA}.people) = 3
    THEN 'ok: refused writes left nothing' ELSE error('a refused write left something behind') END;

DETACH lh;

-- ============================================================
-- The workspace: <db>.<lakehouse>.<schema>.<table>
-- ============================================================

ATTACH '${WS}' AS ws (TYPE delta_classic, READ_ONLY false);

SELECT CASE WHEN (SELECT SUM(amount) FROM ws."${LH_ID}".${SCHEMA}.orders) = 71.0 THEN 'ok: workspace read'
    ELSE error('workspace: wrong sum of orders') END;

CREATE TABLE ws."${LH_ID}".${SCHEMA}.daily AS
SELECT day, SUM(amount) AS amount FROM ws."${LH_ID}".${SCHEMA}.orders GROUP BY day;

USE ws."${LH_ID}".${SCHEMA};
CREATE TABLE unqualified AS SELECT name FROM people;
INSERT INTO unqualified SELECT name || '2' FROM people;
SELECT CASE WHEN (SELECT COUNT(*) FROM unqualified) = 6 AND (SELECT COUNT(*) FROM daily) = 4
    THEN 'ok: USE' ELSE error('USE: wrong rows') END;
USE memory;

SELECT CASE WHEN (SELECT COUNT(*) FROM duckdb_schemas()
    WHERE database_name = 'ws' AND schema_name = '${SCHEMA}' AND parent_schema = '${LH_ID}') = 1
    THEN 'ok: duckdb_schemas' ELSE error('duckdb_schemas: the schema is not under its lakehouse') END;

-- Refused: a table directly in the lakehouse, a lakehouse made from DuckDB
.bail off
CREATE TABLE ws."${LH_ID}".outside (i INTEGER);
CREATE SCHEMA ws.not_a_lakehouse_${SCHEMA};
.bail on

SELECT CASE WHEN (SELECT COUNT(*) FROM duckdb_tables() WHERE database_name = 'ws' AND table_name = 'outside') = 0
    THEN 'ok: refused writes left nothing' ELSE error('a refused write left something behind') END;

DETACH ws;

-- ============================================================
-- Read-only again: everything written is there, writes are refused
-- ============================================================

ATTACH '${LH}' AS ro (TYPE delta_classic);

SELECT CASE WHEN (SELECT COUNT(*) FROM duckdb_tables() WHERE database_name = 'ro' AND schema_name = '${SCHEMA}') = 6
    THEN 'ok: six tables' ELSE error('read-only: expected 6 tables in the schema') END;
SELECT CASE WHEN (SELECT SUM(amount) FROM ro.${SCHEMA}.daily) = 71.0 THEN 'ok: daily'
    ELSE error('daily: wrong sum') END;

.bail off
INSERT INTO ro.${SCHEMA}.people VALUES (9, 'zed');
CREATE TABLE ro.${SCHEMA}.x AS SELECT 1 AS i;
.bail on

SELECT CASE WHEN (SELECT COUNT(*) FROM ro.${SCHEMA}.people) = 3 THEN 'ok: read-only'
    ELSE error('read-only: a write went through') END;

DETACH ro;

SELECT 'all OneLake checks passed' AS result;
