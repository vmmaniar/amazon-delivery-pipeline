"""Pipeline CLI.  python -m pipeline.run all --csv data/raw/amazon_delivery.csv

Steps (each is also an Airflow task): init -> load -> stage -> dq -> core -> marts -> reconcile
"""
import argparse
import sys
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
    """Every raw row must be either loaded to core or quarantined. Fails loudly otherwise."""
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM raw.amazon_delivery WHERE batch_id=%s", (batch_id,))
        raw = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM quarantine.rejected_trip WHERE batch_id=%s", (batch_id,))
        rejected = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM core.fact_trip WHERE batch_id=%s", (batch_id,))
        loaded = cur.fetchone()[0]
    result = {"raw": raw, "loaded": loaded, "rejected": rejected}
    if raw != loaded + rejected:
        raise RuntimeError(f"Reconciliation failed: {result}")
    return result


def print_dq(conn, batch_id):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT rule_code, severity, rows_failed, pct_failed FROM marts.dq_summary "
            "WHERE batch_id=%s AND rows_failed > 0 ORDER BY severity, rows_failed DESC",
            (batch_id,),
        )
        for code, sev, n, pct in cur.fetchall():
            print(f"  {sev:5} {code:24} {n:>6}  ({pct}%)")


def main(argv=None):
    p = argparse.ArgumentParser(prog="pipeline")
    p.add_argument("step", choices=["init", "load", "stage", "dq", "core", "marts", "reconcile", "all"])
    p.add_argument("--csv", type=Path, default=DEFAULT_CSV)
    p.add_argument("--batch-id", type=int, help="batch to operate on (default: latest)")
    p.add_argument("--force", action="store_true", help="reload a CSV that was already landed")
    a = p.parse_args(argv)

    conn = db.connect()
    try:
        if a.step in ("init", "all"):
            init(conn)
        batch_id = a.batch_id
        if a.step in ("load", "all"):
            batch_id, new = load_csv(conn, a.csv, force=a.force)
            print(f"batch {batch_id}: {'loaded' if new else 'already landed, reprocessing'} {a.csv.name}")
        if batch_id is None and a.step not in ("init", "marts"):
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
            print_dq(conn, batch_id)
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
