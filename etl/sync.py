"""Incremental and full sync: list S3 by date, compute daily rollups, write monthly rollup parquet."""

from __future__ import annotations

import csv
import io
import json
import logging
import os
from datetime import date, datetime, timedelta
from typing import Iterator

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore import UNSIGNED
from botocore.config import Config

from src.config import get_settings

from etl.state import get_last_sync_date, set_last_sync_date

log = logging.getLogger(__name__)

ROLLUP_DIR = "rollup"

GroupKey = tuple[str, str, str, date]
MonthKey = tuple[int, int]
MonthAccum = dict[GroupKey, tuple[int, set[str]]]
MonthCounts = dict[GroupKey, tuple[int, int]]


def _s3_client():
    return boto3.client(
        "s3",
        region_name=get_settings().S3_REGION,
        config=Config(signature_version=UNSIGNED),
    )


def _min_date_in_bucket() -> date | None:
    all_dates = _list_dates_full()
    return min(all_dates) if all_dates else None


def _list_dates_incremental() -> list[date]:
    last = get_last_sync_date()
    log.info("State: last_sync_date=%s", last.isoformat() if last else "none")
    settings = get_settings()
    if last is not None:
        start = last
    elif settings.START_DATE:
        start = date.fromisoformat(settings.START_DATE)
        log.info("Using START_DATE: %s", start)
    else:
        log.info("Discovering min date from S3...")
        start = _min_date_in_bucket()
        if start is None:
            log.warning("No day prefixes found in S3")
            return []
        log.info("Earliest date in S3: %s", start)
    end = date.today() - timedelta(days=1)
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
    client = _s3_client()
    settings = get_settings()
    paginator = client.get_paginator("list_objects_v2")
    dates = []
    year_prefixes = set()
    for page in paginator.paginate(
        Bucket=settings.S3_BUCKET, Prefix=f"{settings.S3_PREFIX}/", Delimiter="/"
    ):
        for p in page.get("CommonPrefixes", []):
            prefix = p["Prefix"]
            parts = prefix.rstrip("/").split("/")
            if len(parts) == 3:
                year_prefixes.add(prefix)
    for year_prefix in sorted(year_prefixes):
        for page in paginator.paginate(
            Bucket=settings.S3_BUCKET, Prefix=year_prefix, Delimiter="/"
        ):
            for p in page.get("CommonPrefixes", []):
                month_prefix = p["Prefix"]
                parts = month_prefix.rstrip("/").split("/")
                if len(parts) != 4:
                    continue
                for page2 in paginator.paginate(
                    Bucket=settings.S3_BUCKET, Prefix=month_prefix, Delimiter="/"
                ):
                    for p2 in page2.get("CommonPrefixes", []):
                        day_prefix = p2["Prefix"]
                        parts_day = day_prefix.rstrip("/").split("/")
                        if len(parts_day) != 5:
                            continue
                        try:
                            y, m, d = int(parts_day[2]), int(parts_day[3]), int(parts_day[4])
                            dates.append(date(y, m, d))
                        except (ValueError, IndexError):
                            pass
    log.info("Full refresh: %d day prefixes in S3", len(dates))
    return sorted(dates)


def _list_csv_keys_for_date(d: date) -> list[str]:
    settings = get_settings()
    prefix = f"{settings.S3_PREFIX}/{d.year:04d}/{d.month:02d}/{d.day:02d}/"
    client = _s3_client()
    paginator = client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=settings.S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            k = obj["Key"]
            if k.endswith(".csv"):
                keys.append(k)
    return keys


def _date_batches() -> Iterator[tuple[date, list[str]]]:
    settings = get_settings()
    dates = _list_dates_full() if settings.is_full_refresh else _list_dates_incremental()
    for d in dates:
        keys = _list_csv_keys_for_date(d)
        if not keys:
            continue
        uris = [f"s3://{settings.S3_BUCKET}/{k}" for k in keys]
        yield d, uris


def _split_s3_uri(uri: str) -> tuple[str, str]:
    no_scheme = uri.removeprefix("s3://")
    bucket, key = no_scheme.split("/", 1)
    return bucket, key


def _parse_time_to_day(raw_time: str) -> date | None:
    if raw_time is None:
        return None
    text = raw_time.strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _merge_counts_into_accum(target: MonthAccum, source: MonthAccum) -> None:
    for key, (point_count, file_set) in source.items():
        prev = target.get(key)
        if prev is None:
            target[key] = (point_count, set(file_set))
        else:
            target[key] = (prev[0] + point_count, prev[1] | file_set)


def _record_failed_csvs(
    settings,
    d: date,
    failures: list[dict[str, str]],
    total_files: int,
) -> None:
    payload = {
        "day": d.isoformat(),
        "total_files": total_files,
        "failed_files": len(failures),
        "failures": failures,
    }
    rel_key = f"_state/failed_csvs/day={d.isoformat()}.json"
    if settings.is_local_storage:
        out_path = os.path.join(settings.LOCAL_STORAGE_PATH, settings.GCS_PREFIX, rel_key)
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        log.warning("Wrote failure manifest %s", out_path)
        return

    from google.cloud import storage

    client = storage.Client()
    blob = client.bucket(settings.GCS_BUCKET).blob(f"{settings.GCS_PREFIX}/{rel_key}")
    blob.upload_from_string(json.dumps(payload, indent=2), content_type="application/json")
    log.warning(
        "Wrote failure manifest gs://%s/%s/%s",
        settings.GCS_BUCKET,
        settings.GCS_PREFIX,
        rel_key,
    )


def _process_day(d: date, uris: list[str]) -> MonthAccum:
    """Read CSVs for one source day and return month-partitioned rollup deltas."""
    log.info("Processing day %s (%d CSV files)", d.isoformat(), len(uris))
    settings = get_settings()
    client = _s3_client()
    month_accum: dict[MonthKey, MonthAccum] = {}
    failed_files: list[dict[str, str]] = []
    bad_time_rows = 0

    for uri in uris:
        try:
            bucket, key = _split_s3_uri(uri)
            body = client.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8", errors="replace")
            reader = csv.DictReader(io.StringIO(body))
            for row in reader:
                unique_id = (row.get("UNIQUE_ID") or "").strip()
                file_uuid = (row.get("FILE_UUID") or "").strip()
                platform_name = (row.get("PLATFORM_NAME") or "").strip()
                provider = (row.get("PROVIDER") or "").strip()
                day_val = _parse_time_to_day(row.get("TIME") or "")
                if day_val is None:
                    bad_time_rows += 1
                    continue
                mk = (day_val.year, day_val.month)
                if mk not in month_accum:
                    month_accum[mk] = {}
                key_tuple: GroupKey = (unique_id, platform_name, provider, day_val)
                prev = month_accum[mk].get(key_tuple)
                if prev is None:
                    month_accum[mk][key_tuple] = (1, {file_uuid} if file_uuid else set())
                else:
                    files = set(prev[1])
                    if file_uuid:
                        files.add(file_uuid)
                    month_accum[mk][key_tuple] = (prev[0] + 1, files)
        except Exception as file_exc:
            failed_files.append({"uri": uri, "error": str(file_exc)})
            log.error("Skipping CSV for day %s: %s :: %s", d.isoformat(), uri, file_exc)

    if failed_files:
        _record_failed_csvs(settings, d, failures=failed_files, total_files=len(uris))
    if bad_time_rows:
        log.warning("Day %s: dropping %d row(s) with invalid TIME", d.isoformat(), bad_time_rows)
    if not month_accum:
        log.warning("Day %s: no rows ingested; continuing", d.isoformat())
    return month_accum


def _to_month_counts(month_accum: MonthAccum) -> MonthCounts:
    out: MonthCounts = {}
    for key, (points, files) in month_accum.items():
        out[key] = (int(points), int(len(files)))
    return out


def _partition_rel_path(year: int, month: int) -> str:
    return f"{ROLLUP_DIR}/year={year}/month={month:02d}/data.parquet"


def _local_partition_path(settings, year: int, month: int) -> str:
    rel = _partition_rel_path(year, month)
    return os.path.abspath(os.path.join(settings.LOCAL_STORAGE_PATH, settings.GCS_PREFIX, rel))


def _gcs_partition_key(settings, year: int, month: int) -> str:
    rel = _partition_rel_path(year, month)
    return f"{settings.GCS_PREFIX}/{rel}"


def _read_partition_counts(settings, year: int, month: int) -> MonthCounts:
    counts: MonthCounts = {}
    if settings.is_local_storage:
        path = _local_partition_path(settings, year, month)
        if not os.path.exists(path):
            return counts
        table = pq.read_table(path)
    else:
        import fsspec

        fs = fsspec.filesystem("gcs")
        key = _gcs_partition_key(settings, year, month)
        full = f"{settings.GCS_BUCKET}/{key}"
        if not fs.exists(full):
            return counts
        with fs.open(full, "rb") as f:
            table = pq.read_table(f)

    unique_ids = table.column("unique_id").to_pylist()
    platform_names = table.column("platform_name").to_pylist()
    providers = table.column("provider").to_pylist()
    days = table.column("day").to_pylist()
    point_counts = table.column("point_count").to_pylist()
    file_counts = table.column("file_count").to_pylist()

    for i in range(len(unique_ids)):
        key: GroupKey = (unique_ids[i], platform_names[i], providers[i], days[i])
        counts[key] = (int(point_counts[i]), int(file_counts[i]))
    return counts


def _write_partition_counts(settings, year: int, month: int, counts: MonthCounts) -> None:
    keys_sorted = sorted(counts.keys(), key=lambda k: (k[3], k[0], k[1], k[2]))
    rows_unique_id = [k[0] for k in keys_sorted]
    rows_platform = [k[1] for k in keys_sorted]
    rows_provider = [k[2] for k in keys_sorted]
    rows_day = [k[3] for k in keys_sorted]
    rows_point = [counts[k][0] for k in keys_sorted]
    rows_file = [counts[k][1] for k in keys_sorted]

    table = pa.table(
        {
            "unique_id": pa.array(rows_unique_id, type=pa.string()),
            "platform_name": pa.array(rows_platform, type=pa.string()),
            "provider": pa.array(rows_provider, type=pa.string()),
            "day": pa.array(rows_day, type=pa.date32()),
            "point_count": pa.array(rows_point, type=pa.int64()),
            "file_count": pa.array(rows_file, type=pa.int64()),
        }
    )

    if settings.is_local_storage:
        out_path = _local_partition_path(settings, year, month)
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        pq.write_table(table, out_path, compression="snappy")
        log.info("Wrote %s", out_path.replace("\\", "/"))
    else:
        import fsspec

        fs = fsspec.filesystem("gcs")
        key = _gcs_partition_key(settings, year, month)
        full = f"{settings.GCS_BUCKET}/{key}"
        with fs.open(full, "wb") as f:
            pq.write_table(table, f, compression="snappy")
        log.info("Wrote gs://%s", full)


def _flush_pending_months(settings, pending: dict[MonthKey, MonthAccum]) -> None:
    for (year, month), month_accum in sorted(pending.items()):
        new_counts = _to_month_counts(month_accum)
        existing = _read_partition_counts(settings, year, month)
        # Overwrite semantics for overlapping keys, keep existing non-overlapping keys.
        merged = dict(existing)
        merged.update(new_counts)
        _write_partition_counts(settings, year, month, merged)


def run_sync() -> date | None:
    """
    Run sync (incremental or full): list dates, aggregate rollup per source day,
    write rollup parquet by month, update state checkpoint after each source-month flush.
    Returns max source day processed, or None if no data was processed.
    """
    log.info("Starting processing: S3 read -> monthly rollup parquet")
    settings = get_settings()
    max_date: date | None = None
    day_count = 0
    pending_by_target_month: dict[MonthKey, MonthAccum] = {}
    current_source_period: MonthKey | None = None
    current_state = get_last_sync_date()
    source_checkpoint_day: date | None = None

    for d, uris in _date_batches():
        src_period = (d.year, d.month)
        if current_source_period is None:
            current_source_period = src_period
        elif src_period != current_source_period:
            _flush_pending_months(settings, pending_by_target_month)
            pending_by_target_month.clear()
            if source_checkpoint_day is not None and (
                current_state is None or source_checkpoint_day > current_state
            ):
                set_last_sync_date(source_checkpoint_day)
                current_state = source_checkpoint_day
                log.info(
                    "Checkpointed state after source month %s-%02d: %s",
                    current_source_period[0],
                    current_source_period[1],
                    source_checkpoint_day.isoformat(),
                )
            current_source_period = src_period

        day_delta = _process_day(d, uris)
        for month_key, month_accum in day_delta.items():
            if month_key not in pending_by_target_month:
                pending_by_target_month[month_key] = {}
            _merge_counts_into_accum(pending_by_target_month[month_key], month_accum)

        day_count += 1
        max_date = d
        source_checkpoint_day = d

    if day_count > 0:
        _flush_pending_months(settings, pending_by_target_month)
        if source_checkpoint_day is not None and (
            current_state is None or source_checkpoint_day > current_state
        ):
            set_last_sync_date(source_checkpoint_day)
            log.info(
                "Checkpointed state after source month %s-%02d: %s",
                source_checkpoint_day.year,
                source_checkpoint_day.month,
                source_checkpoint_day.isoformat(),
            )

    log.info("Processed %d day(s)", day_count)
    return max_date
