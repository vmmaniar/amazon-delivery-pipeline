"""Replay trips from the raw CSV into a Kafka topic, in order-time order.

  python streaming/producer.py --csv data/raw/amazon_delivery.csv --limit 5000 --speed 0
  python streaming/producer.py --dry-run --limit 5      # print events, no broker needed

--speed N replays N times faster than real time (0 = as fast as possible).
Events are the raw text fields on purpose: the consumer applies the same DQ rules as batch.
"""
import argparse
import csv
import json
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load_events(path: Path, limit: int | None):
    with open(path, newline="", encoding="utf-8-sig") as f:
        rows = [{k.strip().lower(): v for k, v in r.items()} for r in csv.DictReader(f)]

    def ts(r):
        try:
            return datetime.fromisoformat(f"{r['order_date'].strip()} {r['order_time'].strip()}")
        except ValueError:
            return datetime.max   # unparseable timestamps go last; the pipeline will quarantine them

    rows.sort(key=ts)
    return rows[:limit] if limit else rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", type=Path, default=ROOT / "data" / "raw" / "amazon_delivery.csv")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="delivery.trips")
    ap.add_argument("--limit", type=int)
    ap.add_argument("--speed", type=float, default=0.0)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    events = load_events(a.csv, a.limit)
    if a.dry_run:
        for e in events[:5]:
            print(json.dumps(e))
        print(f"... {len(events)} events would be sent to {a.topic}")
        return

    from kafka import KafkaProducer
    prod = KafkaProducer(bootstrap_servers=a.bootstrap, value_serializer=lambda v: json.dumps(v).encode(),
                         key_serializer=lambda k: k.encode(), linger_ms=20)
    prev = None
    for e in events:
        cur = None
        try:
            cur = datetime.fromisoformat(f"{e['order_date'].strip()} {e['order_time'].strip()}")
        except ValueError:
            pass
        if a.speed > 0 and prev and cur and cur > prev:
            time.sleep((cur - prev).total_seconds() / a.speed)
        if cur:
            prev = cur
        prod.send(a.topic, key=e["order_id"] or "", value=e)
    prod.flush()
    print(f"sent {len(events)} events to {a.topic}")


if __name__ == "__main__":
    main()
