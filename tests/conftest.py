import os
import sys
from pathlib import Path

import psycopg
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

ADMIN_URL = os.environ.get("TEST_ADMIN_URL", "postgresql://dev:dev@localhost:5432/postgres")
TEST_DB = "delivery_test"

HEADER = ("Order_ID,Agent_Age,Agent_Rating,Store_Latitude,Store_Longitude,Drop_Latitude,Drop_Longitude,"
          "Order_Date,Order_Time,Pickup_Time,Weather,Traffic,Vehicle,Area,Delivery_Time,Category")

# One good row plus one row per defect. Store is Bengaluru unless stated.
ROWS = {
    "good":        "g1,30,4.5,12.9716,77.5946,12.9816,77.6046,2022-03-19,11:30:00,11:45:00,Sunny,High ,motorcycle ,Urban ,60,Clothing",
    "ws_case":     "g2,28,4.1,12.9716,77.5946,12.9816,77.6046,2022-03-19,11:30:00,11:40:00,  sunny ,low,SCOOTER,Metropolitian ,45,Books",
    "dup":         "g1,30,4.5,12.9716,77.5946,12.9816,77.6046,2022-03-19,11:30:00,11:45:00,Sunny,High ,motorcycle ,Urban ,60,Clothing",
    "age15":       "b_age,15,4.5,12.9716,77.5946,12.9816,77.6046,2022-03-19,11:30:00,11:45:00,Sunny,High ,bicycle ,Urban ,60,Home",
    "rating6":     "b_rate,30,6.0,12.9716,77.5946,12.9816,77.6046,2022-03-19,11:30:00,11:45:00,Sunny,High ,van,Urban ,60,Home",
    "store0":      "b_zero,30,4.5,0.0,0.0,0.01,0.01,2022-03-19,11:30:00,11:45:00,Sunny,High ,van,Urban ,60,Home",
    "outside":     "b_geo,30,4.5,51.5,0.1,51.6,0.2,2022-03-19,11:30:00,11:45:00,Sunny,High ,van,Urban ,60,Home",
    "no_time":     "b_time,30,4.5,12.9716,77.5946,12.9816,77.6046,2022-03-19,NaN ,11:45:00,NaN ,NaN ,van,Urban ,60,Home",
    "bad_deliv":   "b_deliv,30,4.5,12.9716,77.5946,12.9816,77.6046,2022-03-19,11:30:00,11:45:00,Sunny,High ,van,Urban ,0,Home",
    "far":         "b_far,30,4.5,12.9716,77.5946,19.0,72.8,2022-03-19,11:30:00,11:45:00,Sunny,High ,van,Urban ,60,Home",
    "late_pickup": "b_pick,30,4.5,12.9716,77.5946,12.9816,77.6046,2022-03-19,06:00:00,10:30:00,Sunny,High ,van,Urban ,60,Home",
    "midnight":    "w_mid,30,4.5,12.9716,77.5946,12.9816,77.6046,2022-03-19,23:55:00,00:05:00,Sunny,High ,van,Urban ,60,Home",
    "sign":        "w_sign,30,4.5,-12.9716,77.5946,12.9816,77.6046,2022-03-19,11:30:00,11:45:00,Sunny,High ,van,Urban ,60,Home",
    "missing":     "w_miss,30,,12.9716,77.5946,12.9816,77.6046,2022-03-19,11:30:00,11:45:00,NaN,NaN ,van,Urban ,60,Home",
}


@pytest.fixture(scope="session")
def test_dsn():
    with psycopg.connect(ADMIN_URL, autocommit=True) as c:
        c.execute(f"DROP DATABASE IF EXISTS {TEST_DB} WITH (FORCE)")
        c.execute(f"CREATE DATABASE {TEST_DB}")
    base = ADMIN_URL.rsplit("/", 1)[0]
    dsn = f"{base}/{TEST_DB}"
    os.environ["DATABASE_URL"] = dsn
    yield dsn
    with psycopg.connect(ADMIN_URL, autocommit=True) as c:
        c.execute(f"DROP DATABASE IF EXISTS {TEST_DB} WITH (FORCE)")


@pytest.fixture(scope="session")
def fixture_csv(tmp_path_factory):
    p = tmp_path_factory.mktemp("data") / "fixture.csv"
    p.write_text(HEADER + "\n" + "\n".join(ROWS.values()) + "\n")
    return p


@pytest.fixture(scope="session")
def ran(test_dsn, fixture_csv):
    """Run the whole pipeline once on the fixture; tests inspect the result."""
    from pipeline import db, run
    run.main(["all", "--csv", str(fixture_csv)])
    conn = db.connect(autocommit=True)   # no open txn => no locks against the re-run test
    yield conn
    conn.close()
