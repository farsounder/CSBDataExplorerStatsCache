"""Regression tests for file-driven additive incremental sync."""

from __future__ import annotations

from datetime import date

import pyarrow.parquet as pq

from etl.sync import run_sync
from tests.conftest import make_csv_body, rows_for


def _read_partition(local_storage, year: int, month: int) -> dict:
    path = (
        local_storage
        / "test"
        / "rollup"
        / f"year={year}"
        / f"month={month:02d}"
        / "data.parquet"
    )
    if not path.exists():
        return {}
    table = pq.read_table(str(path))
    out: dict = {}
    for i in range(table.num_rows):
        key = (
            table.column("unique_id")[i].as_py(),
            table.column("platform_name")[i].as_py(),
            table.column("provider")[i].as_py(),
            table.column("day")[i].as_py(),
        )
        out[key] = (
            table.column("point_count")[i].as_py(),
            table.column("file_count")[i].as_py(),
        )
    return out


def test_late_arriving_file_updates_earlier_logical_month(
    local_storage, fake_s3, freeze_today, monkeypatch
):
    monkeypatch.setenv("START_DATE", "2026-01-15")
    from src import config

    config.get_settings.cache_clear()

    fake_s3({
        "csb/csv/2026/01/15/a.csv": make_csv_body(
            rows_for("p1", "FILE-A", "2026-01-15T12:00:00Z", 50)
        ),
    })
    freeze_today(date(2026, 1, 16))
    run_sync()

    jan = _read_partition(local_storage, 2026, 1)
    assert jan[("p1", "Plat", "Prov", date(2026, 1, 15))] == (50, 1)

    fake_s3({
        "csb/csv/2026/01/15/a.csv": make_csv_body(
            rows_for("p1", "FILE-A", "2026-01-15T12:00:00Z", 50)
        ),
        "csb/csv/2026/04/10/late.csv": make_csv_body(
            rows_for("p1", "FILE-B", "2026-01-15T08:00:00Z", 30)
        ),
    })
    freeze_today(date(2026, 4, 17))
    run_sync()

    jan = _read_partition(local_storage, 2026, 1)
    assert jan[("p1", "Plat", "Prov", date(2026, 1, 15))] == (80, 2)


def test_rerunning_with_no_new_files_is_a_noop(
    local_storage, fake_s3, freeze_today, monkeypatch
):
    monkeypatch.setenv("START_DATE", "2026-04-15")
    from src import config

    config.get_settings.cache_clear()

    fake_s3({
        "csb/csv/2026/04/15/a.csv": make_csv_body(
            rows_for("p1", "FILE-A", "2026-04-15T10:00:00Z", 100)
        ),
    })
    freeze_today(date(2026, 4, 17))
    run_sync()
    first = _read_partition(local_storage, 2026, 4)

    run_sync()
    second = _read_partition(local_storage, 2026, 4)

    assert first == second
    assert second[("p1", "Plat", "Prov", date(2026, 4, 15))] == (100, 1)


def test_two_source_days_contribute_to_same_logical_day(
    local_storage, fake_s3, freeze_today, monkeypatch
):
    monkeypatch.setenv("START_DATE", "2026-04-15")
    from src import config

    config.get_settings.cache_clear()

    fake_s3({
        "csb/csv/2026/04/15/a.csv": make_csv_body(
            rows_for("p1", "FILE-A", "2026-04-15T10:00:00Z", 100)
        ),
        "csb/csv/2026/04/16/b.csv": make_csv_body(
            rows_for("p1", "FILE-B", "2026-04-15T22:00:00Z", 50)
        ),
    })
    freeze_today(date(2026, 4, 17))
    run_sync()

    apr = _read_partition(local_storage, 2026, 4)
    assert apr[("p1", "Plat", "Prov", date(2026, 4, 15))] == (150, 2)
