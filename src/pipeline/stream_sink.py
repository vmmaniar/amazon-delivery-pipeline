"""Micro-batch sink: lands trip events (dicts of raw text fields) and runs them through the
same stage -> dq -> core SQL as the batch path, so streaming and batch share one rule set."""
import hashlib
import json

import psycopg

from . import run
from .load_raw import RAW_COLUMNS


def land_events(conn: psycopg.Connection, events: list[dict], source: str = "kafka") -> int:
    """Insert events as a new raw batch and return its batch_id."""
    if not events:
        raise ValueError("no events")
    payload = json.dumps(events, sort_keys=True).encode()
    digest = hashlib.sha256(payload).hexdigest()
    with conn.cursor() as cur:
        cur.execute("SELECT batch_id FROM ops.load_batch WHERE file_sha256 = %s", (digest,))
        if (row := cur.fetchone()):
            return row[0]          # replayed micro-batch: already landed
        cur.execute(
            "INSERT INTO ops.load_batch (source_file, file_sha256) VALUES (%s, %s) RETURNING batch_id",
            (source, digest),
        )
        batch_id = cur.fetchone()[0]
        with cur.copy(
            f"COPY raw.amazon_delivery (batch_id, row_num, {', '.join(RAW_COLUMNS)}) FROM STDIN"
        ) as cp:
            for i, ev in enumerate(events, start=1):
                cp.write_row([batch_id, i, *[ev.get(c) for c in RAW_COLUMNS]])
        cur.execute(
            "UPDATE ops.load_batch SET row_count=%s, status='loaded', finished_at=now() WHERE batch_id=%s",
            (len(events), batch_id),
        )
    conn.commit()
    return batch_id


def process_events(conn: psycopg.Connection, events: list[dict], source: str = "kafka") -> dict:
    batch_id = land_events(conn, events, source)
    run.stage(conn, batch_id)
    run.dq(conn, batch_id)
    run.core(conn, batch_id)
    return {"batch_id": batch_id, **run.reconcile(conn, batch_id)}
