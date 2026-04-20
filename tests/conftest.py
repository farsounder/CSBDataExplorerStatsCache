"""Shared fixtures for ETL regression tests."""

from __future__ import annotations

import csv
import io
from datetime import date
from unittest.mock import MagicMock

import pytest

from src import config


@pytest.fixture(autouse=True)
def _reset_settings_cache():
    config.get_settings.cache_clear()
    yield
    config.get_settings.cache_clear()


@pytest.fixture
def local_storage(tmp_path, monkeypatch):
    """Point ETL at a clean local storage directory."""
    monkeypatch.setenv("LOCAL_STORAGE_PATH", str(tmp_path))
    monkeypatch.setenv("GCS_PREFIX", "test")
    monkeypatch.delenv("GCS_BUCKET", raising=False)
    monkeypatch.delenv("SYNC_MODE", raising=False)
    monkeypatch.delenv("FULL_REFRESH", raising=False)
    monkeypatch.delenv("START_DATE", raising=False)
    config.get_settings.cache_clear()
    return tmp_path


def make_csv_body(rows: list[dict]) -> str:
    buf = io.StringIO()
    fieldnames = ["UNIQUE_ID", "FILE_UUID", "PLATFORM_NAME", "PROVIDER", "TIME"]
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return buf.getvalue()


def rows_for(unique_id: str, file_uuid: str, time_iso: str, count: int) -> list[dict]:
    return [
        {
            "UNIQUE_ID": unique_id,
            "FILE_UUID": file_uuid,
            "PLATFORM_NAME": "Plat",
            "PROVIDER": "Prov",
            "TIME": time_iso,
        }
        for _ in range(count)
    ]


class FakeS3:
    def __init__(self, files: dict[str, str]):
        self.files = dict(files)

    def get_paginator(self, _op):
        outer = self

        class _Paginator:
            def paginate(self, **kwargs):
                prefix = kwargs.get("Prefix", "")
                contents = [
                    {"Key": k}
                    for k in sorted(outer.files.keys())
                    if k.startswith(prefix)
                ]
                yield {"Contents": contents}

        return _Paginator()

    def get_object(self, Bucket, Key):  # noqa: N803 - matches boto3 API
        body = self.files[Key].encode("utf-8")
        return {"Body": MagicMock(read=lambda: body)}


@pytest.fixture
def fake_s3(monkeypatch):
    """Install a controllable in-memory S3 client into etl.sync."""
    state = {"client": FakeS3({})}

    def install(files: dict[str, str]) -> FakeS3:
        state["client"] = FakeS3(files)
        return state["client"]

    from etl import sync as sync_mod

    monkeypatch.setattr(sync_mod, "_s3_client", lambda: state["client"])
    return install


@pytest.fixture
def freeze_today(monkeypatch):
    """Replace date.today() in etl.sync with a controllable value."""
    from etl import sync as sync_mod

    class _FrozenDate(date):
        _today_value: date = date(2026, 4, 17)

        @classmethod
        def today(cls):
            return cls._today_value

    def set_today(today: date) -> None:
        _FrozenDate._today_value = today
        monkeypatch.setattr(sync_mod, "date", _FrozenDate)

    set_today(date(2026, 4, 17))
    return set_today
