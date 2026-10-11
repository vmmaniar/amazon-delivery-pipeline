# Amazon Delivery Pipeline

A last-mile delivery data pipeline built on the public **Amazon Delivery** dataset (43,739 orders across Indian cities: store and drop locations, order/pickup times, agent age and rating, traffic, weather, vehicle type, delivery time).

The raw file is messy in realistic ways, so the point of the project is the engineering around it: land the data untouched, clean it in SQL, check it against explicit rules, **quarantine the rows that fail instead of dropping them**, and serve a star schema.

```
 CSV ──► raw.amazon_delivery ──► staging.trip_clean ──► core (star schema) ──► marts.* views
 (text)   verbatim, per batch     typed, normalised,       fact_trip + 7 dims     daily KPIs, vehicle,
                                  violation codes/row            │                traffic×weather, city
                                         │                       │
                                         └──► quarantine.rejected_trip   ops.dq_result (counts per rule)
                                              (raw row kept as JSON)

 Orchestration: Airflow DAG (@daily)        Streaming (optional): CSV ─► Kafka ─► micro-batch ─► same SQL
```

## Results on the full dataset

| | rows |
|---|---|
| Raw rows landed | 43,739 |
| Loaded to `core.fact_trip` | **40,153** |
| Quarantined (failed an error rule) | **3,586** |
| Reconciliation | raw = loaded + quarantined, asserted on every run |

What the rules found in the real file:

| Rule | Severity | Rows | What it is |
|---|---|---|---|
| `E_STORE_COORD_ZERO` | error | 3,505 | store lat/lon is `0` (drop coords are then ~`0.01`) |
| `E_ORDER_TS_INVALID` | error | 91 | order time is the text `"NaN "` |
| `E_RATING_RANGE` | error | 53 | agent rating of `6.0` on a 1–5 scale (all 53 are agents aged exactly 50) |
| `E_AGENT_AGE_RANGE` | error | 38 | agents aged 15 (all 38 also have a rating of exactly 1.0) |
| `W_PICKUP_NEXT_DAY` | warn | 828 | order just before midnight, pickup after; pickup rolled to next day instead of showing a −1,430 min delay |
| `W_COORD_SIGN_FIXED` | warn | 188 | latitude or longitude with a flipped sign; repaired with `abs()` (distances become plausible, max 21 km) |
| `W_WEATHER_MISSING` / `W_TRAFFIC_MISSING` | warn | 91 each | `"NaN"` text; loaded as `Unknown` |
| `W_RATING_MISSING` | warn | 54 | loaded as `NULL` |

All 15 `bicycle` rows are corrupted: each has a missing order time and is also an age-15 (8) or rating-6 (7) row, so `bicycle` never reaches the clean data.

Also normalised: trailing spaces (`"High "`, `"motorcycle "`), the source spelling `Metropolitian`, mixed case.

Bengaluru has **3,047** clean trips from 20 stores (stores are assigned to the nearest reference city within 60 km).

## What the clean data says

Full write-up with charts: **[docs/FINDINGS.md](docs/FINDINGS.md)** and a single-file [dashboard](docs/dashboard.html) (download and open in a browser). Both are generated from the warehouse by `make report`, so no number is typed by hand.

- **Traffic:** Low 101 min vs Jam 148 min (+46%).
- **Weather:** Sunny 104 min vs Cloudy 138 / Fog 137; ratings barely change.
- **Distance:** 102 min under 3 km rising to 142 min at 12+ km.
- **Time of day:** 20:00 orders are slowest (149 min); 08:00-10:59 fastest (92-94 min).
- **Category:** Grocery averages 26 min; every other category is 129-133 min.
- **Agents:** under-30s are rated 4.68 and take 109 min; 30+ are rated 4.59 and take 141 min.
- **Vehicle:** motorcycles are about 15 min slower than scooters and vans at every traffic level.

These are associations, not causes, and several are unusually clean for real-world data (see the caveats in the findings).

![traffic](docs/charts/traffic.png)

## Design decisions

- **Raw is text, verbatim.** Nothing is cast or fixed on the way in, so any later question about "what did the source say" is answerable. Each load is a `batch_id`; the file's SHA-256 is unique, so the same file is never landed twice.
- **Rules are data, not scattered `if`s.** `ops.dq_rule` is the catalogue (`error` quarantines the row, `warn` loads it and counts it). Stage computes a `violations text[]` per row, and `ops.dq_result` stores one count per rule per batch, including zeros.
- **Quarantine, not delete.** `quarantine.rejected_trip` keeps the error codes and the original row as `jsonb`, so rejects can be inspected and replayed after a rule or upstream fix.
- **Idempotent everywhere.** Staging/quarantine/DQ results are replaced per batch; core is upserted on `order_id`. Re-running a step or the whole pipeline gives the same state (covered by a test).
- **Streaming reuses the batch rules.** The Kafka consumer lands micro-batches into `raw` and calls the same stage → dq → core SQL, so there is one definition of "valid".
- **The DAG has no business logic.** Tasks call the same functions as the CLI. A DQ gate fails the run if more than 15% of a batch is quarantined.

## Star schema

`core.fact_trip` (one row per trip: timestamps, distance, pickup delay, delivery minutes, speed, DQ warnings, batch) joined to `dim_date`, `dim_store` (→ `dim_city`), `dim_vehicle`, `dim_weather`, `dim_traffic`, `dim_area`, `dim_category`.
The dataset has no agent id, so agent age/rating live on the fact table rather than a made-up agent dimension.

Marts: `marts.v_trip` (flat), `daily_kpis`, `vehicle_performance`, `traffic_weather_impact`, `area_traffic`, `city_performance`, `hourly_demand`, `distance_band`, `category_performance`, `age_band_rating`, `rating_distribution`, `headline`, `dq_summary`.

```sql
-- how much slower is a "Jam" than "Low" traffic?
SELECT traffic, round(avg(delivery_minutes),1) AS avg_min, count(*) FROM marts.v_trip GROUP BY 1 ORDER BY 2;
--  Low 101.4 · Medium 127.0 · High 129.5 · Jam 147.9
```

## Run it

Requires Python 3.11+ and Postgres 16 (Docker or local).

```bash
pip install -r requirements.txt -r requirements-report.txt   # report deps only needed for `make report`
docker compose up -d postgres          # or point DATABASE_URL at any Postgres
./scripts/fetch_data.sh                # full dataset (~6 MB); a 2,000-row sample is already in data/sample/
make run                               # load -> stage -> dq -> core -> marts -> reconcile
make report                            # charts + docs/FINDINGS.md + docs/dashboard.html (needs requirements-report.txt)
make test                              # 23 tests, creates/drops scratch databases
```

`DATABASE_URL` defaults to `postgresql://dev:dev@localhost:5432/delivery`. Individual steps: `python -m pipeline.run {init|load|stage|dq|core|marts|reconcile|all}` (run from `src/`, or set `PYTHONPATH=src`).

### Airflow

```bash
docker compose --profile airflow up -d   # UI on http://localhost:8080
```

DAG `delivery_pipeline`: `init_schema → load_raw_file → stage → dq_checks → load_core → refresh_marts / reconcile`. Pass another file with `--conf '{"csv": "/opt/project/data/other.csv"}'`.

### Kafka replay (optional)

```bash
docker compose --profile streaming up -d
pip install -r requirements-streaming.txt
python streaming/consumer.py &                                   # micro-batches into Postgres
python streaming/producer.py --limit 5000 --speed 0              # replay in order-time order
python streaming/producer.py --dry-run --limit 3                 # inspect events without a broker
```

## What is and isn't verified

- **Tested** (23 pytest cases against real Postgres, also run in GitHub Actions): every DQ rule, normalisation, midnight rollover, sign repair, city assignment, quarantine contents, reconciliation, re-run idempotency, the streaming sink with a replayed micro-batch, the analysis views, and report generation from the sample file.
- **Run end to end** on the full 43,739-row file.
- **Airflow DAG**: parsed and run end to end with `airflow dags test` on Airflow 3.0.6 (all 6 tasks succeeded). Not run on a long-lived scheduler.
- **Kafka producer/consumer**: **not run against a real broker.** Only the shared sink they call (`pipeline.stream_sink`) is tested, and the producer's `--dry-run` output was checked. Treat the Kafka path as a working draft.

## Source

Dataset: Amazon Delivery Dataset (Kaggle: `sujalsuralkar/amazon-delivery-dataset`). The fetch script uses a public GitHub mirror of the same CSV because Kaggle needs a login. Check the dataset's licence on Kaggle before redistributing the full file; only a 2,000-row sample is committed here.
