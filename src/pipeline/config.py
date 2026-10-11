import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SQL_DIR = ROOT / "sql"
DEFAULT_CSV = ROOT / "data" / "raw" / "amazon_delivery.csv"


def dsn() -> str:
    """Postgres connection string. Override with DATABASE_URL."""
    return os.environ.get("DATABASE_URL", "postgresql://dev:dev@localhost:5432/delivery")
