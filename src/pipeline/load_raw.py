"""Land a CSV in raw.amazon_delivery exactly as it is (all text), tagged with a batch id."""
import csv
import hashlib
from pathlib import Path

import psycopg

RAW_COLUMNS = [
    "order_id", "agent_age", "agent_rating", "store_latitude", "store_longitude",
    "drop_latitude", "drop_longitude", "order_date", "order_time", "pickup_time",
    "weather", "traffic", "vehicle", "area", "delivery_time", "category",
]


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def load_csv(conn: psycopg.Connection, path: Path, force: bool = False) -> tuple[int, bool]:
    """Returns (batch_id, newly_loaded). The same file content is only landed once unless force."""
    path = Path(path)
    digest = sha256_of(path)
    with conn.cursor() as cur:
        cur.execute("SELECT batch_id FROM ops.load_batch WHERE file_sha256 = %s", (digest,))
        row = cur.fetchone()
        if row and not force:
            return row[0], False
        if row:  # force: wipe the previous landing of this exact file
            cur.execute("DELETE FROM raw.amazon_delivery WHERE batch_id = %s", (row[0],))
            batch_id = row[0]
            cur.execute("UPDATE ops.load_batch SET status='loading', finished_at=NULL WHERE batch_id=%s", (batch_id,))
        else:
            cur.execute(
                "INSERT INTO ops.load_batch (source_file, file_sha256) VALUES (%s, %s) RETURNING batch_id",
                (path.name, digest),
            )
            batch_id = cur.fetchone()[0]

        with open(path, newline="", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            header = [h.strip().lower() for h in next(reader)]
            if header != RAW_COLUMNS:
                raise ValueError(f"Unexpected header {header}; expected {RAW_COLUMNS}")
            n = 0
            with cur.copy(
                f"COPY raw.amazon_delivery (batch_id, row_num, {', '.join(RAW_COLUMNS)}) FROM STDIN"
            ) as cp:
                for n, rec in enumerate(reader, start=1):
                    if len(rec) != len(RAW_COLUMNS):
                        raise ValueError(f"Row {n} has {len(rec)} fields, expected {len(RAW_COLUMNS)}")
                    cp.write_row([batch_id, n, *rec])
        cur.execute(
            "UPDATE ops.load_batch SET row_count=%s, status='loaded', finished_at=now() WHERE batch_id=%s",
            (n, batch_id),
        )
    conn.commit()
    return batch_id, True
