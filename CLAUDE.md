# Delta Classic - DuckDB Extension

## What This Is

A DuckDB C++ StorageExtension that registers `TYPE delta_classic` for `ATTACH`. It lets you attach a directory of Delta tables as a single database instead of attaching each table individually. It does zero Delta reading itself — everything delegates to the existing Delta extension via internal `ATTACH ... (TYPE DELTA)`.

## Architecture

```
ATTACH 'path' AS db (TYPE delta_classic, PIN_SNAPSHOT)
  → DeltaClassicCatalog(base_path, pin_snapshot)
    → DiscoverSchemas: FileSystem::ListFiles + _delta_log check
      → DeltaClassicSchemaEntry per schema dir
        → DeltaClassicTableSet: lazy list of subdirs with _delta_log
          → DeltaClassicTableEntry: on first query, internally runs
            ATTACH 'path/table' AS __dc_internal (TYPE DELTA, PIN_SNAPSHOT)
            then delegates GetScanFunction to that internal table
```

### Schema Discovery
- If immediate children have `_delta_log` → **single schema** (name = last path component)
- Otherwise → **multi-schema** (each child dir = schema, grandchildren = tables)

### Key Files
| File | What It Does |
|---|---|
| `src/delta_classic_extension.cpp` | Entry point. Registers `delta_classic` storage type, parses PIN_SNAPSHOT from ATTACH options |
| `src/storage/delta_classic_catalog.cpp` | Virtual catalog. Scans directory to discover schemas (single vs multi mode) |
| `src/storage/delta_classic_schema_entry.cpp` | Read-only schema. Delegates table lookups to table set |
| `src/storage/delta_classic_table_set.cpp` | Lists subdirs via `FileSystem::ListFiles`, checks `_delta_log` existence |
| `src/storage/delta_classic_table_entry.cpp` | The core: internally ATTACHes each delta table on first access, delegates scan |
| `src/storage/delta_classic_transaction*.cpp` | No-op read-only transaction plumbing |

### Key Design Decisions
1. **Internal ATTACH, not delta_scan** — `PIN_SNAPSHOT` only works with `ATTACH TYPE DELTA`, not `delta_scan()` directly
2. **Lazy everything** — schemas discovered on first lookup, tables listed on first access, internal ATTACH on first query
3. **FileSystem abstraction** — works with local, S3, ABFSS, GCS paths automatically
4. **Read-only** — all write operations throw

## Building

```bash
make release        # Build
make test           # Run tests
```

Targets DuckDB v1.4.4. Uses `extension-ci-tools` and `duckdb` as git submodules (both from `duckdb/` org, branch `main`).

## CI

GitHub Actions workflow at `.github/workflows/MainDistributionPipeline.yml` — builds on all platforms using `duckdb/extension-ci-tools` reusable workflows.

## Dependencies

- **Runtime**: Requires the Delta extension to be loaded (`LOAD delta`)
- **Build**: No external libraries. Pure C++ against DuckDB headers
