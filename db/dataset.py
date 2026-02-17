"""
Generic Kaggle Dataset Loader

Downloads any Kaggle dataset and creates a DuckDB database.
Configuration is read from environment variables (set in .env at the project root):
  - KAGGLE_DATASET : Kaggle dataset slug  
  - DB_NAME        : Name for the database file

The resulting .db file is placed in the db/ directory.
"""

import os
from pathlib import Path

import duckdb
import kagglehub
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")
DB_DIR = Path(__file__).resolve().parent  


def _get_env(key: str, default: str | None = None) -> str:
    """Read a required env var, raising if missing and no default."""
    val = (os.getenv(key) or "").strip()
    if val:
        return val
    if default is not None:
        return default
    raise RuntimeError(f"Environment variable {key} is not set. Add it to .env")


def resolve_db_path(db_path: str | None = None) -> str:
    """
    Resolve the database path.

    Priority:
      1) Explicit db_path argument (if provided)
      2) DB_NAME env var  →  db/<DB_NAME>.db
      3) Falls back to db/database.db
    """
    if db_path and str(db_path).strip():
        return str(db_path).strip()

    db_name = (os.getenv("DB_NAME") or "").strip() or "database"
    return str(DB_DIR / f"{db_name}.db")


class KaggleDataset:
    """
    Download a Kaggle dataset and load into DuckDB.
    """

    def __init__(self, db_path: str | None = None):
        self.db_path = resolve_db_path(db_path)
        self.dataset = _get_env("KAGGLE_DATASET")
        self.download_path = self._download_data()
        self._create_db()

    def _download_data(self) -> str:
        """Download the dataset from Kaggle and return the local path."""
        path = kagglehub.dataset_download(self.dataset)
        print(f"Downloaded dataset '{self.dataset}' to {path}")
        return path

    def _create_db(self):
        """Create DuckDB tables from every CSV in the downloaded directory."""
        con = duckdb.connect(database=self.db_path, read_only=False)
        csv_count = 0
        for fname in os.listdir(self.download_path):
            if not fname.lower().endswith(".csv"):
                continue
            csv_count += 1
            fpath = os.path.join(self.download_path, fname)
            table_name = os.path.splitext(fname)[0]
            con.execute(
                f"CREATE TABLE IF NOT EXISTS {table_name} AS "
                f"SELECT * FROM read_csv_auto('{fpath}');"
            )
        con.close()
        print(f"Created {csv_count} tables in {self.db_path}")

def get_schema_info(db_path: str | None = None) -> dict:
    """
    Retrieve schema information for all tables in the database.

    Returns:
        dict mapping table names to lists of {"name": ..., "type": ...} dicts.
    """
    resolved = resolve_db_path(db_path)
    con = duckdb.connect(database=resolved, read_only=True)
    try:
        tables = con.execute("SHOW TABLES").fetchall()
        schema_info = {}
        for (table_name,) in tables:
            columns = con.execute(f"DESCRIBE {table_name}").fetchall()
            schema_info[table_name] = [
                {"name": col[0], "type": col[1]} for col in columns
            ]
        return schema_info
    finally:
        con.close()


if __name__ == "__main__":
    print("Initializing database from Kaggle dataset...")
    ds = KaggleDataset()

    print("\nDatabase Schema:")
    print("-" * 50)
    schema = get_schema_info()
    for table, columns in schema.items():
        print(f"\nTable: {table}")
        for col in columns:
            print(f"  - {col['name']}: {col['type']}")
