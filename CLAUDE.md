# Delta Classic - DuckDB Extension

## What This Is

A DuckDB C++ StorageExtension that registers `TYPE delta_classic` for `ATTACH`. It lets you attach a directory of Delta tables as a single database instead of attaching each table individually. It does zero Delta reading itself — everything delegates to the existing Delta extension via internal `ATTACH ... (TYPE DELTA)`.

## Architecture

```
ATTACH 'path' AS db (TYPE delta_classic, PIN_SNAPSHOT, TIMESTAMP ..., PUSHDOWN_FILTERS ...)
  → DeltaClassicCatalog(base_path, delta_options)
    → DiscoverSchemas: DeltaClassicListDirectory (delta_classic_path.cpp)
      → DeltaClassicSchemaEntry per schema dir
        → DeltaClassicTableSet: one "listed" DeltaClassicTableEntry per table (no columns)
          → on lookup, Resolve(): internally runs (once, re-done after a ROLLBACK)
            ATTACH 'path/table' AS __dc_<catalog id>_<n> (TYPE DELTA, HIDDEN, READ_ONLY or READ_WRITE + child_catalog_mode, internal_table_name <table>, <delta_options>)
            then looks the table up there (passing the AT clause) and returns a per-transaction
            "resolved" DeltaClassicTableEntry that copies its columns and forwards scans/stats to it
```

### Schema Discovery
- If the path itself has `_delta_log` → **single table** (`main.<last path component>`)
- If immediate children have `_delta_log` → **single schema** (named `main`)
- Otherwise → **multi-schema** (each child dir = schema, grandchildren = tables)

### Listing styles (important for S3)
- Local and Azure `ListFiles` are flat: one entry per child, with `is_directory` set.
- httpfs (S3/GCS) `ListFiles` is a recursive `**` glob: every object key below the directory, relative to it, with `is_directory=false`. `S3FileSystem::DirectoryExists` returns true for any prefix, so never use it to probe for `_delta_log`.
- `DeltaClassicListDirectory` handles both; a recursive listing of the base path also yields every schema's tables, so schemas aren't listed again.

### Key Files
| File | What It Does |
|---|---|
| `src/delta_classic_extension.cpp` | Entry point. Registers `delta_classic` storage type, parses PIN_SNAPSHOT from ATTACH options |
| `src/storage/delta_classic_catalog.cpp` | Virtual catalog. Discovers schemas (single table / single / multi schema), hands out internal DB names, detaches them on DETACH |
| `src/storage/delta_classic_path.cpp` | Path helpers and `DeltaClassicListDirectory` (flat and recursive listings) |
| `src/storage/delta_classic_schema_entry.cpp` | Schema. Delegates table lookups to the table set; forwards CREATE TABLE to a new internal Delta attach |
| `src/storage/delta_classic_table_set.cpp` | Lists the tables of a schema (unless discovery already did) |
| `src/storage/delta_classic_table_entry.cpp` | The core: internal ATTACH per table, per-transaction resolution, forwards scans/stats/virtual columns/AT clause |
| `src/storage/delta_classic_transaction*.cpp` | Transactions; cache the resolved table entries of a transaction (the Delta extension commits writes itself) |

### Key Design Decisions
1. **Internal ATTACH, not delta_scan** — `PIN_SNAPSHOT` only works with `ATTACH TYPE DELTA`, not `delta_scan()` directly
2. **Lazy everything** — schemas discovered on first lookup, tables listed on first access, internal ATTACH on first query
3. **FileSystem abstraction** — works with local, S3, ABFSS, GCS paths automatically
4. **Read-only by default** — `READ_ONLY false` makes the database writable: each internal ATTACH is then `READ_WRITE` with `child_catalog_mode`, `PlanInsert` forwards to the inner `DeltaCatalog` (which resolves its own table entry), and `CreateTable` calls the inner `main` schema's `CreateTable`. Only the delta_classic database is registered as modified; the inner Delta transactions commit with it. The access mode must be applied with `AttachedDatabase::SetReadOnlyDatabase()`: changing `options.access_mode` in the attach callback has no effect
5. **Delta options go in `AttachInfo::options`** — the Delta extension reads its options (PIN_SNAPSHOT, TIMESTAMP, ...) from the `AttachInfo`, not from `AttachOptions`
6. **Per-transaction table entries** — like duckdb-delta, so columns match the snapshot being read and nothing shared is mutated

## Building & Testing

**There is no local build or test environment.** All building and testing happens via CI (GitHub Actions). Do not attempt to build or run tests locally — just push to trigger CI.

Targets DuckDB v2.0.0. Uses `extension-ci-tools` and `duckdb` as git submodules (both from `duckdb/` org). DuckDB 2.0 only.

## CI/CD

GitHub Actions workflow at `.github/workflows/MainDistributionPipeline.yml`, identical to upstream [duckdb/extension-template](https://github.com/duckdb/extension-template): runs on every push and PR, builds and tests on all platforms (`_extension_distribution.yml`) and runs the code quality check (`_extension_code_quality.yml`: clang-format, black, clang-tidy). Keep the build files (`CMakeLists.txt`, `Makefile`, `extension_config.cmake`, `vcpkg.json`, `.clang-format`/`.clang-tidy`/`.editorconfig` symlinks into `duckdb/`) in line with the template.

### Formatting
The format check uses clang-format 11.0.1 and black 24 with DuckDB's config. To fix locally (a lint pass, not a build): in a venv with `clang_format==11.0.1 'black==24.*' cmake-format`, copy `duckdb/.clang-format` to the repo root temporarily (the symlink isn't materialized on Windows, and without it clang-format silently uses LLVM style), run `python duckdb/scripts/format.py --all --fix --noconfirm --workdir <repo> --directories src test`, then `git checkout -- .clang-format`.

### Critical: CI rebuilds DuckDB from source on every push
The CI pipeline compiles DuckDB from source (not prebuilt binaries) for every architecture on every push. This means each push triggers 15-30+ minutes of builds across linux_amd64, linux_arm64, linux_amd64_musl, macOS, Windows, and WASM. **Batch your changes and only push when ready.** Don't make multiple small commits pushed individually — it wastes CI time and can create cascading cancelled runs.

### Architectures
WASM (`wasm_mvp;wasm_eh;wasm_threads`) is excluded in `MainDistributionPipeline.yml` and must be excluded in the community-extensions descriptor (`excluded_platforms`): the Delta extension has no WASM build, so delta_classic can't work there (issue #1). Every other architecture is required: if a build fails on one, fix the root cause, don't exclude it.

### vcpkg.json is required
Even though this extension has no external dependencies, `vcpkg.json` must exist with the overlay-ports and overlay-triplets configuration. Without it, CI fails with "Failed to load manifest from directory" errors.

### Workflow pinning
The workflows use `extension-ci-tools@v2.0-cyanoptera`, with `duckdb_version: v2.0.0` and `ci_tools_version: v2.0-cyanoptera`. The duckdb submodule is at the `v2.0.0` tag and extension-ci-tools at the `v2.0-cyanoptera` branch.

## Dependencies

- **Runtime**: Requires the Delta extension to be loaded (`LOAD delta`). duckdb-delta `main` is the DuckDB 2.0 line.

## Supported versions
DuckDB v2.0.0 only, from `main`. Older DuckDB versions (the `v1.5`, `v145`, `v144` branches) are not supported and get no fixes.
- **Build**: No external libraries. Pure C++ against DuckDB headers.

## Development Rules

### Always verify APIs against local headers before writing code
DuckDB has no stable C++ API. Methods, signatures, and includes change between versions. The `duckdb` submodule at `duckdb/src/include/` has all v2.0 headers locally. **Before using any DuckDB API, grep the local headers to confirm it exists:**
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
4. **Write enforcement** — CREATE TABLE and INSERT only work with `READ_ONLY false`; every other write path (UPDATE, DELETE, DROP, ALTER, CREATE SCHEMA, CTAS) must throw, because the Delta extension can't do it either. DuckDB will call these methods; they must exist but reject.

## Reference Extensions

- [duckdb/duckdb-delta](https://github.com/duckdb/duckdb-delta) (branch `main`) — the Delta extension this delegates to
- [hugr-lab/mssql-extension](https://github.com/hugr-lab/mssql-extension) — example community extension with proper repo structure
