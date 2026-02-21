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

## Building & Testing

**There is no local build or test environment.** All building and testing happens via CI (GitHub Actions). Do not attempt to build or run tests locally — just push to trigger CI.

Targets DuckDB v1.5.0. Uses `extension-ci-tools` and `duckdb` as git submodules (both from `duckdb/` org). The `v144` branch preserves compatibility with DuckDB v1.4.4.

## CI/CD

GitHub Actions workflow at `.github/workflows/MainDistributionPipeline.yml` — builds on all platforms using `duckdb/extension-ci-tools` reusable workflows.

### Critical: CI rebuilds DuckDB from source on every push
The CI pipeline compiles DuckDB from source (not prebuilt binaries) for every architecture on every push. This means each push triggers 15-30+ minutes of builds across linux_amd64, linux_arm64, linux_amd64_musl, macOS, Windows, and WASM. **Batch your changes and only push when ready.** Don't make multiple small commits pushed individually — it wastes CI time and can create cascading cancelled runs.

### All architectures are required
Do NOT exclude any architecture from the CI matrix. The extension must build on all platforms to be accepted into the DuckDB community extensions repo. If a build fails on one arch, fix the root cause — don't exclude the arch.

### vcpkg.json is required
Even though this extension has no external dependencies, `vcpkg.json` must exist with the overlay-ports and overlay-triplets configuration. Without it, CI fails with "Failed to load manifest from directory" errors.

### Workflow pinning
The workflow, `duckdb_version`, and `ci_tools_version` are all pinned to `v1.5.0`. The duckdb submodule tracks the `v1.5-variegata` branch and extension-ci-tools tracks the `v1.5.0` branch.

## Dependencies

- **Runtime**: Requires the Delta extension to be loaded (`LOAD delta`).
- **Build**: No external libraries. Pure C++ against DuckDB headers.

## Development Rules

### Always verify APIs against local headers before writing code
DuckDB has no stable C++ API. Methods, signatures, and includes change between versions. The `duckdb` submodule at `duckdb/src/include/` has all v1.5 headers locally. **Before using any DuckDB API, grep the local headers to confirm it exists:**
```bash
# Check if a method exists
grep "TryGetContext" duckdb/src/include/duckdb/catalog/catalog_transaction.hpp
# Find where a class is defined
grep -r "class BinderException" duckdb/src/include/
# Check method signatures
grep -A 5 "GetScanFunction" duckdb/src/include/duckdb/catalog/catalog_entry/table_catalog_entry.hpp
```
This takes seconds vs 20+ minutes per CI round-trip. Never guess at APIs.

### Never put #include inside namespace blocks
`#include` directives must go before `namespace duckdb {`, not inside it. Putting them inside causes types to be declared in a nested `duckdb::duckdb` namespace.

## Testing Rules

### Never reference internal databases in tests
The `__dc_*` internal databases are an implementation detail. Tests must ONLY interact with the user-facing catalog (`ATTACH 'path' AS db (TYPE delta_classic)` then query `db.schema.table`). Never assert on, query, or reference `__dc_*` database names. If something doesn't work through the public interface, fix the C++ code — don't test the internals.

## Common Pitfalls

1. **PIN_SNAPSHOT is ATTACH-only** — it's an option for `ATTACH ... (TYPE DELTA)`, not a parameter for `delta_scan()`. This is why the extension internally ATTACHes each table rather than calling delta_scan directly.
2. **Don't fork extension-ci-tools** — community extensions have their own standalone repos (e.g., `djouallah/delta_classic`), not forks of `duckdb/extension-ci-tools`.
3. **Submodule init** — after cloning, run `git submodule update --init --recursive` to pull duckdb and extension-ci-tools.
4. **Read-only enforcement** — every write path (CREATE TABLE, INSERT, UPDATE, DELETE, DROP) must throw. DuckDB will call these methods; they must exist but reject.

## Reference Extensions

- [duckdb/duckdb-delta](https://github.com/duckdb/duckdb-delta) — the Delta extension this delegates to
- [hugr-lab/mssql-extension](https://github.com/hugr-lab/mssql-extension) — example community extension with proper repo structure
