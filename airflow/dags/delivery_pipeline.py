"""Daily delivery pipeline.

  load_raw -> stage -> dq_checks -> load_core -> refresh_marts -> reconcile

Each task calls the same functions as `python -m pipeline.run`, so the DAG adds scheduling,
retries and visibility but no business logic. The batch_id is passed between tasks via XCom.
Trigger with a different file:  airflow dags trigger delivery_pipeline --conf '{"csv": "/data/new.csv"}'
"""
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, task

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from pipeline import config, db, load_raw, run  # noqa: E402

DEFAULT_ARGS = {"owner": "data-eng", "retries": 2, "retry_delay": timedelta(minutes=2)}


@dag(
    dag_id="delivery_pipeline",
    schedule="@daily",
    start_date=datetime(2022, 2, 11),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["delivery", "postgres"],
    params={"csv": str(config.DEFAULT_CSV)},
)
def delivery_pipeline():
    @task
    def init_schema():
        with db.connect() as conn:
            run.init(conn)

    @task
    def load_raw_file(params=None) -> int:
        with db.connect() as conn:
            batch_id, _ = load_raw.load_csv(conn, Path(params["csv"]))
            return batch_id

    @task
    def stage(batch_id: int) -> int:
        with db.connect() as conn:
            run.stage(conn, batch_id)
        return batch_id

    @task
    def dq_checks(batch_id: int) -> int:
        with db.connect() as conn:
            run.dq(conn, batch_id)
            # Fail the run if more than 15% of rows were quarantined: something upstream broke.
            rejected, total = conn.execute(
                "SELECT count(*) FILTER (WHERE EXISTS (SELECT 1 FROM quarantine.rejected_trip q "
                "WHERE q.batch_id = s.batch_id AND q.row_num = s.row_num)), count(*) "
                "FROM staging.trip_clean s WHERE batch_id = %s", (batch_id,)).fetchone()
            if total == 0 or rejected / total > 0.15:
                raise ValueError(f"DQ gate failed: {rejected}/{total} rows quarantined")
        return batch_id

    @task
    def load_core(batch_id: int) -> int:
        with db.connect() as conn:
            run.core(conn, batch_id)
        return batch_id

    @task
    def refresh_marts():
        with db.connect() as conn:
            run.marts(conn)

    @task
    def reconcile(batch_id: int) -> dict:
        with db.connect() as conn:
            return run.reconcile(conn, batch_id)

    init = init_schema()
    b = load_raw_file()
    init >> b
    b = stage(b)
    b = dq_checks(b)
    b = load_core(b)
    refresh_marts() << b
    reconcile(b)


delivery_pipeline()
