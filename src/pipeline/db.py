import psycopg
from psycopg import ClientCursor

from .config import SQL_DIR, dsn


def connect(autocommit: bool = False) -> psycopg.Connection:
    return psycopg.connect(dsn(), autocommit=autocommit)


def run_sql_file(conn: psycopg.Connection, name: str, params: dict | None = None) -> None:
    """Execute one file from sql/. Params use %(name)s placeholders."""
    sql = (SQL_DIR / name).read_text()
    # ClientCursor binds params client-side, which allows several statements per file
    with ClientCursor(conn) as cur:
        cur.execute(sql, params or {})
