"""Incremental and full sync: list S3 by date, DuckDB read CSV -> write Parquet per partition."""

from __future__ import annotations

import logging
import os
import tempfile
from datetime import date, timedelta
from typing import Iterator

import boto3

log = logging.getLogger(__name__)
from botocore import UNSIGNED
from botocore.config import Config

from src.config import get_settings
from src.db import connection_etl

from etl.state import get_last_sync_date, set_last_sync_date

# ROW_GROUP_SIZE per plan (122880 rows) for predicate pushdown
ROW_GROUP_SIZE = 122880

# NOAA public bucket
S3_BUCKET = "noaa-dcdb-bathymetry-pds"
S3_PREFIX = "csb/csv"


def _s3_client():
    return boto3.client(
        "s3",
        region_name=get_settings().S3_REGION,
        config=Config(signature_version=UNSIGNED),
    )


def _min_date_in_bucket() -> date | None:
    """Earliest partition date present in the S3 bucket, or None if empty."""
    all_dates = _list_dates_full()
    return min(all_dates) if all_dates else None


def _list_dates_incremental() -> list[date]:
    """Dates to process: after last_sync_date up to yesterday."""
    last = get_last_sync_date()
    log.info("State: last_sync_date=%s", last.isoformat() if last else "none")
    settings = get_settings()
    if last is not None:
        start = last + timedelta(days=1)
    elif settings.START_DATE:
        start = date.fromisoformat(settings.START_DATE)
        log.info("Using START_DATE: %s", start)
    else:
        log.info("Discovering min date from S3...")
        start = _min_date_in_bucket()
        if start is None:
            log.warning("No partition dates found in S3 bucket")
            return []
        log.info("Earliest date in S3: %s", start)
    end = date.today() - timedelta(days=1)  # yesterday
    if start > end:
        log.info("No dates to process (start=%s > end=%s)", start, end)
        return []
    out = []
    d = start
    while d <= end:
        out.append(d)
        d += timedelta(days=1)
    log.info("Incremental date range: %s to %s (%d days)", start, end, len(out))
    return out


def _list_dates_full() -> list[date]:
    """Discover all partition dates in S3 by listing prefixes (year -> month -> day)."""
    client = _s3_client()
    paginator = client.get_paginator("list_objects_v2")
    dates = []
    # List years under csb/csv/
    year_prefixes = set()
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=f"{S3_PREFIX}/", Delimiter="/"):
        for p in page.get("CommonPrefixes", []):
            prefix = p["Prefix"]
            # prefix is like "csb/csv/2019/"
            parts = prefix.rstrip("/").split("/")
            if len(parts) == 3:
                year_prefixes.add(prefix)
    for year_prefix in sorted(year_prefixes):
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=year_prefix, Delimiter="/"):
            for p in page.get("CommonPrefixes", []):
                month_prefix = p["Prefix"]
                # csb/csv/2019/06/
                parts = month_prefix.rstrip("/").split("/")
                if len(parts) == 4:
                    for page2 in paginator.paginate(
                        Bucket=S3_BUCKET, Prefix=month_prefix, Delimiter="/"
                    ):
                        for p2 in page2.get("CommonPrefixes", []):
                            day_prefix = p2["Prefix"]
                            parts_day = day_prefix.rstrip("/").split("/")
                            if len(parts_day) == 5:
                                try:
                                    y, m, d = int(parts_day[2]), int(parts_day[3]), int(parts_day[4])
                                    dates.append(date(y, m, d))
                                except (ValueError, IndexError):
                                    pass
    log.info("Full refresh: %d partition dates in S3", len(dates))
    return sorted(dates)


def _list_csv_keys_for_date(d: date) -> list[str]:
    """Return S3 keys (prefix-relative) for a given partition date."""
    prefix = f"{S3_PREFIX}/{d.year:04d}/{d.month:02d}/{d.day:02d}/"
    client = _s3_client()
    paginator = client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            k = obj["Key"]
            if k.endswith(".csv"):
                keys.append(k)
    return keys


def _date_batches() -> Iterator[tuple[date, list[str]]]:
    """Yield (date, list of s3 URIs) for each date to process."""
    if get_settings().is_full_refresh:
        dates = _list_dates_full()
    else:
        dates = _list_dates_incremental()
    for d in dates:
        keys = _list_csv_keys_for_date(d)
        if not keys:
            continue
        uris = [f"s3://{S3_BUCKET}/{k}" for k in keys]
        yield d, uris


def _run_partition(con, d: date, uris: list[str]) -> None:
    """Read CSVs from S3 for one date, write Hive-partitioned Parquet to GCS or local path."""
    log.info("Processing partition %s (%d CSV files)", d.isoformat(), len(uris))
    settings = get_settings()
    y, m, day = d.year, d.month, d.day
    # DuckDB: read_csv_auto from list of S3 URIs; cast TIME to TIMESTAMP (ISO 8601)
    uris_sql = ",".join([f"'{u}'" for u in uris])
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE staging AS
        SELECT
            UNIQUE_ID,
            FILE_UUID,
            LON,
            LAT,
            DEPTH,
            CAST(TIME AS TIMESTAMP) AS TIME,
            PLATFORM_NAME,
            PROVIDER
        FROM read_csv_auto([{uris_sql}])
    """)

    if settings.is_local_storage:
        base = settings.storage_base_uri
        out_path = f"{base}/year={y}/month={m:02d}/day={day:02d}/data.parquet"
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        out_path = os.path.abspath(out_path).replace("\\", "/")
        con.execute(f"""
            COPY staging TO '{out_path}'
            (FORMAT PARQUET, ROW_GROUP_SIZE {ROW_GROUP_SIZE})
        """)
        log.info("Wrote %s", out_path)
    else:
        # GCS: write to temp file then upload with gcsfs (uses default credentials on Cloud Run)
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            tmp = f.name
        try:
            con.execute(f"""
                COPY staging TO '{tmp}'
                (FORMAT PARQUET, ROW_GROUP_SIZE {ROW_GROUP_SIZE})
            """)
            gcs_path = f"{settings.GCS_BUCKET}/{settings.GCS_PREFIX}/year={y}/month={m:02d}/day={day:02d}/data.parquet"
            import fsspec
            fs = fsspec.filesystem("gcs")
            fs.put(tmp, gcs_path)
            log.info("Wrote gs://%s", gcs_path)
        finally:
            try:
                os.unlink(tmp)
            except OSError:
                pass

    con.execute("DROP TABLE staging")


def run_sync() -> date | None:
    """
    Run sync (incremental or full): list dates, process each partition, update state.
    Returns the max date written, or None if no data was processed.
    """
    log.info("Connecting (S3 read, GCS/local write)...")
    con = connection_etl()
    try:
        max_date: date | None = None
        batch_count = 0
        for d, uris in _date_batches():
            _run_partition(con, d, uris)
            batch_count += 1
            if max_date is None or d > max_date:
                max_date = d
        if max_date is not None:
            log.info("Updating state: last_sync_date=%s", max_date.isoformat())
            set_last_sync_date(max_date)
        log.info("Processed %d partition(s)", batch_count)
        return max_date
    finally:
        con.close()
