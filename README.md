# Amazon delivery pipeline

A small data pipeline over the public Amazon Delivery dataset (43,739 last-mile orders across Indian cities: store and drop coordinates, order and pickup times, agent age and rating, traffic, weather, vehicle, delivery time).

The raw CSV is messy, so most of the work is cleaning it and being clear about what gets thrown out and why:

```
CSV -> raw.amazon_delivery -> staging.trip_clean -> core (star schema) -> marts views
       text, as in the file    typed, normalised,     fact_trip + dims
                               rule violations per row
                                      |
                                      +-> quarantine.rejected_trip   (bad rows, original kept as JSON)
                                      +-> ops.dq_result              (failure count per rule per batch)
```

Everything runs in Postgres SQL; Python just loads the file and runs the SQL files in order. An Airflow DAG runs it daily.

## Result on the full file

43,739 raw rows -> 40,153 loaded, 3,586 quarantined. Each run checks that raw = loaded + quarantined.

| Rule | Rows | What it caught |
|---|---|---|
| `E_STORE_COORD_ZERO` | 3,505 | store lat/lon is 0 |
| `E_ORDER_TS_INVALID` | 91 | order time is the text `"NaN "` |
| `E_RATING_RANGE` | 53 | rating of 6 on a 1-5 scale (all from agents aged 50) |
| `E_AGENT_AGE_RANGE` | 38 | agents aged 15 (all rated exactly 1.0) |
| `W_PICKUP_NEXT_DAY` | 828 | order before midnight, pickup after; rolled to the next day |
| `W_COORD_SIGN_FIXED` | 188 | flipped sign on a coordinate; repaired with `abs()` |
| `W_WEATHER_MISSING`, `W_TRAFFIC_MISSING` | 91 each | `"NaN"`; loaded as `Unknown` |
| `W_RATING_MISSING` | 54 | loaded as NULL |

`E_` rules quarantine the row, `W_` rules load it and just count it. The rule list lives in the `ops.dq_rule` table.

Also cleaned: trailing spaces (`"High "`, `"motorcycle "`) and the source's `Metropolitian` spelling. All 15 `bicycle` rows are corrupt (each has a missing order time and is also an age-15 or rating-6 row), so bicycle never makes it into the clean data.

## Schema

`core.fact_trip` is one row per trip, joined to `dim_date`, `dim_store` (-> `dim_city`), `dim_vehicle`, `dim_weather`, `dim_traffic`, `dim_area` and `dim_category`. The dataset has no agent id, so agent age and rating sit on the fact table. Stores are assigned to the nearest reference city within 60 km.

Reporting views are in the `marts` schema (daily KPIs, vehicle, traffic x weather, distance bands, categories, age bands, cities, DQ summary).

Load is idempotent: staging and quarantine are replaced per batch, core is upserted on `order_id`, and the same file (by SHA-256) is never landed twice.

## Findings

[docs/FINDINGS.md](docs/FINDINGS.md) has the charts and numbers, generated from the database by `make report`. Short version:

- Traffic: Low 101 min vs Jam 148 min.
- Weather: Sunny 104 min vs Cloudy 138 and Fog 137.
- Distance: 102 min under 3 km, about 142 min from 12 km up.
- Grocery averages 26 min; every other category is 129-133.
- Agents aged 30+ are rated lower (4.59 vs 4.68) and slower (141 vs 109 min). The step at exactly 30 looks too tidy to be real behaviour, so check how that column was recorded before trusting it.

These are associations in observational data, not causes.

## Running it

Needs Python 3.11+ and Postgres 16.

```bash
pip install -r requirements.txt
docker compose up -d postgres      # or set DATABASE_URL to any Postgres
./scripts/fetch_data.sh            # full CSV; a 2,000-row sample is in data/sample/
make run                           # load, clean, check, build star schema and marts
make report                        # charts + docs/FINDINGS.md
```

`DATABASE_URL` defaults to `postgresql://dev:dev@localhost:5432/delivery`. Single steps: `python -m pipeline.run {init|load|stage|dq|core|marts|reconcile}` with `PYTHONPATH=src`.

**Airflow:** `airflow/dags/delivery_pipeline.py` runs the same steps as tasks (init, load, stage, dq, core, marts, reconcile) and fails if more than 15% of a batch is quarantined. Needs Airflow 3 installed and `DATABASE_URL` set; I ran it with `airflow dags test` on 3.0.6.

## Data

Kaggle: `sujalsuralkar/amazon-delivery-dataset`. `fetch_data.sh` pulls a public GitHub mirror of the same CSV since Kaggle needs a login. Check the dataset's licence before redistributing the full file; only a 2,000-row sample is committed.
