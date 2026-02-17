import os
import pytest
import duckdb


@pytest.fixture(scope="session")
def extension_path():
    path = os.environ.get("DELTA_CLASSIC_EXTENSION_PATH")
    if not path:
        # Fallback: try common local build path
        path = "build/release/extension/delta_classic/delta_classic.duckdb_extension"
    if not os.path.exists(path):
        pytest.skip(f"Extension not found at {path}. Set DELTA_CLASSIC_EXTENSION_PATH.")
    return path


@pytest.fixture
def conn(extension_path):
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute(f"INSTALL '{extension_path}'")
    yield con
    con.close()
