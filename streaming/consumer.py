"""Consume trip events and micro-batch them into Postgres through the shared SQL pipeline.

  python streaming/consumer.py --batch-size 500 --max-wait 10
"""
import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pipeline import db            # noqa: E402
from pipeline.stream_sink import process_events   # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="delivery.trips")
    ap.add_argument("--group", default="delivery-sink")
    ap.add_argument("--batch-size", type=int, default=500)
    ap.add_argument("--max-wait", type=float, default=10.0, help="flush a partial batch after N seconds")
    a = ap.parse_args()

    from kafka import KafkaConsumer
    consumer = KafkaConsumer(a.topic, bootstrap_servers=a.bootstrap, group_id=a.group,
                             enable_auto_commit=False, auto_offset_reset="earliest",
                             value_deserializer=lambda b: json.loads(b.decode()))
    conn = db.connect()
    buf, last_flush = [], time.time()
    while True:
        polled = consumer.poll(timeout_ms=1000, max_records=a.batch_size)
        for records in polled.values():
            buf.extend(r.value for r in records)
        if buf and (len(buf) >= a.batch_size or time.time() - last_flush >= a.max_wait):
            result = process_events(conn, buf)
            consumer.commit()          # commit offsets only after the batch is safely in Postgres
            print(result, flush=True)
            buf, last_flush = [], time.time()


if __name__ == "__main__":
    main()
