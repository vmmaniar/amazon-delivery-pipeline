from pathlib import Path

import pytest

from pipeline import db, run
from pipeline.load_raw import load_csv


def q(conn, sql, *args):
    return conn.execute(sql, args).fetchall()


def test_reconciles_every_row(ran):
    n_raw = q(ran, "SELECT count(*) FROM raw.amazon_delivery")[0][0]
    loaded = q(ran, "SELECT count(*) FROM core.fact_trip")[0][0]
    rejected = q(ran, "SELECT count(*) FROM quarantine.rejected_trip")[0][0]
    assert n_raw == 14 and loaded + rejected == n_raw


@pytest.mark.parametrize("order_id,code", [
    ("b_age", "E_AGENT_AGE_RANGE"),
    ("b_rate", "E_RATING_RANGE"),
    ("b_zero", "E_STORE_COORD_ZERO"),
    ("b_geo", "E_COORD_OUTSIDE_INDIA"),
    ("b_time", "E_ORDER_TS_INVALID"),
    ("b_deliv", "E_DELIVERY_TIME_RANGE"),
    ("b_far", "E_DISTANCE_IMPLAUSIBLE"),
    ("b_pick", "E_PICKUP_DELAY_RANGE"),
])
def test_bad_rows_are_quarantined_with_reason(ran, order_id, code):
    rows = q(ran, "SELECT error_codes FROM quarantine.rejected_trip WHERE order_id = %s", order_id)
    assert rows and code in rows[0][0]
    assert not q(ran, "SELECT 1 FROM core.fact_trip WHERE order_id = %s", order_id)


def test_duplicate_order_keeps_first_only(ran):
    assert q(ran, "SELECT count(*) FROM core.fact_trip WHERE order_id='g1'")[0][0] == 1
    dup = q(ran, "SELECT error_codes FROM quarantine.rejected_trip WHERE order_id='g1'")
    assert dup == [(["E_DUPLICATE_ORDER"],)]


def test_quarantine_keeps_raw_record(ran):
    rec = q(ran, "SELECT raw_record FROM quarantine.rejected_trip WHERE order_id='b_rate'")[0][0]
    assert rec["agent_rating"] == "6.0"


def test_text_is_normalised(ran):
    row = q(ran, "SELECT weather, traffic, vehicle_type, area_type FROM marts.v_trip WHERE order_id='g2'")[0]
    assert row == ("Sunny", "Low", "scooter", "Metropolitan")


def test_nan_tokens_become_unknown_and_null(ran):
    row = q(ran, "SELECT weather, traffic, agent_rating FROM marts.v_trip WHERE order_id='w_miss'")[0]
    assert row == ("Unknown", "Unknown", None)
    warns = q(ran, "SELECT dq_warnings FROM core.fact_trip WHERE order_id='w_miss'")[0][0]
    assert {"W_RATING_MISSING", "W_WEATHER_MISSING", "W_TRAFFIC_MISSING"} <= set(warns)


def test_sign_repair_is_loaded_with_warning(ran):
    lat, warns = q(ran, "SELECT s.lat, f.dq_warnings FROM core.fact_trip f "
                        "JOIN core.dim_store s USING (store_key) WHERE order_id='w_sign'")[0]
    assert float(lat) == 12.9716 and "W_COORD_SIGN_FIXED" in warns


def test_midnight_crossing_rolls_to_next_day(ran):
    delay, pick = q(ran, "SELECT pickup_delay_min, pickup_ts FROM core.fact_trip WHERE order_id='w_mid'")[0]
    assert float(delay) == 10.0 and pick.day == 20


def test_city_assignment_and_distance(ran):
    city, dist = q(ran, "SELECT city_name, distance_km FROM marts.v_trip WHERE order_id='g1'")[0]
    assert city == "Bengaluru" and 1.0 < float(dist) < 2.5


def test_dq_results_cover_every_rule(ran):
    n_rules = q(ran, "SELECT count(*) FROM ops.dq_rule")[0][0]
    assert q(ran, "SELECT count(*) FROM ops.dq_result")[0][0] == n_rules


def test_rerun_is_idempotent(ran, fixture_csv):
    before = q(ran, "SELECT count(*) FROM core.fact_trip")[0][0]
    run.main(["all", "--csv", str(fixture_csv)])     # same file => no new batch
    run.main(["all", "--csv", str(fixture_csv), "--force"])
    assert q(ran, "SELECT count(*) FROM core.fact_trip")[0][0] == before
    assert q(ran, "SELECT count(*) FROM ops.load_batch")[0][0] == 1


def test_bad_header_is_refused(ran, tmp_path):
    bad = tmp_path / "bad.csv"
    bad.write_text("a,b,c\n1,2,3\n")
    with pytest.raises(ValueError):
        load_csv(conn := db.connect(), bad)
    conn.close()


def test_stream_micro_batch_shares_batch_rules(ran):
    from pipeline.stream_sink import process_events
    good = dict(order_id="s1", agent_age="30", agent_rating="4.2", store_latitude="12.9716",
                store_longitude="77.5946", drop_latitude="12.98", drop_longitude="77.60",
                order_date="2022-03-20", order_time="10:00:00", pickup_time="10:10:00", weather="Fog",
                traffic="Jam ", vehicle="motorcycle ", area="Urban ", delivery_time="50", category="Toys")
    bad = {**good, "order_id": "s2", "agent_rating": "6.0"}
    conn = db.connect()
    res = process_events(conn, [good, bad])
    again = process_events(conn, [good, bad])      # replayed micro-batch is a no-op
    conn.close()
    assert res["loaded"] == 1 and res["rejected"] == 1 and again["batch_id"] == res["batch_id"]
    assert q(ran, "SELECT error_codes FROM quarantine.rejected_trip WHERE order_id='s2'") == [(["E_RATING_RANGE"],)]


def test_analysis_views_on_fixture(ran):
    bands = dict(q(ran, "SELECT age_band, trips FROM marts.age_band_rating"))
    assert set(bands) <= {"18-24", "25-29", "30-34", "35-39", "40+"}
    assert sum(bands.values()) == q(ran, "SELECT count(*) FROM core.fact_trip WHERE agent_rating IS NOT NULL")[0][0]
    km = q(ran, "SELECT km_band FROM marts.distance_band")
    assert ("0-3",) in km
    h = q(ran, "SELECT trips, corr_distance_time FROM marts.headline")[0]
    assert h[0] == q(ran, "SELECT count(*) FROM core.fact_trip")[0][0]
    assert q(ran, "SELECT count(*) FROM marts.category_performance")[0][0] >= 2
    assert q(ran, "SELECT count(*) FROM marts.area_traffic")[0][0] >= 2
    assert q(ran, "SELECT count(*) FROM marts.rating_distribution")[0][0] >= 2
