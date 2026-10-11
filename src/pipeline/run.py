"""python -m pipeline.run all --csv data/raw/amazon_delivery.csv

Steps: init -> load -> stage -> dq -> core -> marts -> reconcile (each is also an Airflow task).
"""
import argparse
from pathlib import Path

from . import db
from .config import DEFAULT_CSV
from .load_raw import load_csv

INIT_FILES = ["00_schemas.sql", "01_functions.sql", "10_raw_ops.sql", "20_core_ddl.sql"]


def init(conn):
    for f in INIT_FILES:
        db.run_sql_file(conn, f)
    conn.commit()


def stage(conn, batch_id):
    db.run_sql_file(conn, "30_stage.sql", {"batch_id": batch_id})
    conn.commit()


def dq(conn, batch_id):
    db.run_sql_file(conn, "40_dq.sql", {"batch_id": batch_id})
    conn.commit()


def core(conn, batch_id):
    db.run_sql_file(conn, "50_load_core.sql", {"batch_id": batch_id})
    conn.execute("UPDATE ops.load_batch SET status='transformed' WHERE batch_id=%s", (batch_id,))
    conn.commit()


def marts(conn):
    db.run_sql_file(conn, "60_marts.sql")
    conn.commit()


def reconcile(conn, batch_id) -> dict:
    """Every raw row must be either in core or quarantined."""
    raw, loaded, rejected = conn.execute(
        "SELECT (SELECT count(*) FROM raw.amazon_delivery WHERE batch_id = %(b)s),"
        " (SELECT count(*) FROM core.fact_trip WHERE batch_id = %(b)s),"
        " (SELECT count(*) FROM quarantine.rejected_trip WHERE batch_id = %(b)s)", {"b": batch_id}
    ).fetchone()
    if raw != loaded + rejected:
        raise RuntimeError(f"row count mismatch: raw={raw} loaded={loaded} rejected={rejected}")
    return {"raw": raw, "loaded": loaded, "rejected": rejected}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("step", choices=["init", "load", "stage", "dq", "core", "marts", "reconcile", "all"])
    p.add_argument("--csv", type=Path, default=DEFAULT_CSV)
    a = p.parse_args()

    with db.connect() as conn:
        if a.step in ("init", "all"):
            init(conn)
        if a.step in ("load", "all"):
            batch_id, new = load_csv(conn, a.csv)
            print(f"batch {batch_id}: {'loaded' if new else 'already loaded'} {a.csv.name}")
        batch_id = conn.execute("SELECT max(batch_id) FROM ops.load_batch").fetchone()[0]
        if a.step in ("stage", "all"):
            stage(conn, batch_id)
        if a.step in ("dq", "all"):
            dq(conn, batch_id)
        if a.step in ("core", "all"):
            core(conn, batch_id)
        if a.step in ("marts", "all"):
            marts(conn)
        if a.step in ("reconcile", "all"):
            print(reconcile(conn, batch_id))
            for code, n, pct in conn.execute(
                "SELECT rule_code, rows_failed, pct_failed FROM marts.dq_summary "
                "WHERE batch_id = %s AND rows_failed > 0 ORDER BY rows_failed DESC", (batch_id,)
            ):
                print(f"  {code:24} {n:>6}  ({pct}%)")


if __name__ == "__main__":
    main()
