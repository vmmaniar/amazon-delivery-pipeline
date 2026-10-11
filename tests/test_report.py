"""Builds the report from the committed 2,000-row sample in its own database."""
from pathlib import Path

import psycopg
import pytest

pytest.importorskip("matplotlib")

from conftest import ADMIN_URL  # noqa: E402

SAMPLE = Path(__file__).resolve().parents[1] / "data" / "sample" / "amazon_delivery_sample.csv"
DB = "delivery_test_report"


@pytest.fixture(scope="module")
def report_dir(tmp_path_factory, monkeypatch_module):
    import os
    from pipeline import db, report, run
    with psycopg.connect(ADMIN_URL, autocommit=True) as c:
        c.execute(f"DROP DATABASE IF EXISTS {DB} WITH (FORCE)")
        c.execute(f"CREATE DATABASE {DB}")
    prev = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = ADMIN_URL.rsplit("/", 1)[0] + "/" + DB
    out = tmp_path_factory.mktemp("docs")
    monkeypatch_module.setattr(report, "DOCS", out)
    monkeypatch_module.setattr(report, "CHARTS", out / "charts")
    run.main(["all", "--csv", str(SAMPLE)])
    report.main()
    yield out
    if prev is None:
        os.environ.pop("DATABASE_URL", None)
    else:
        os.environ["DATABASE_URL"] = prev
    with psycopg.connect(ADMIN_URL, autocommit=True) as c:
        c.execute(f"DROP DATABASE IF EXISTS {DB} WITH (FORCE)")


@pytest.fixture(scope="module")
def monkeypatch_module():
    from _pytest.monkeypatch import MonkeyPatch
    m = MonkeyPatch()
    yield m
    m.undo()


def test_report_files_exist(report_dir):
    for n in ("traffic", "weather", "distance", "hour", "age", "category", "vehicle", "dq"):
        assert (report_dir / "charts" / f"{n}.png").stat().st_size > 5_000
    assert (report_dir / "dashboard.html").stat().st_size > 50_000


def test_findings_numbers_come_from_db(report_dir):
    md = (report_dir / "FINDINGS.md").read_text()
    assert "# Findings" in md and "{" not in md           # no unformatted placeholders
    assert "1,827 clean trips" in md and "2,000 raw rows" in md and "173 quarantined" in md
