"""ETL entrypoint: load config (env + CLI), run sync, exit 0 on success."""

from __future__ import annotations

import argparse
import logging
import os
import sys
import traceback

from src.config import get_settings

from etl.sync import run_sync

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
    stream=sys.stdout,
    force=True,
)
log = logging.getLogger(__name__)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="NOAA CSB Compressor: sync S3 CSV -> GCS Parquet")
    p.add_argument(
        "--full-refresh",
        action="store_true",
        help="Re-process all S3 partition dates and overwrite the Parquet store",
    )
    p.add_argument(
        "--start-date",
        metavar="YYYY-MM-DD",
        help="First date for initial incremental run when no state exists",
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    if args.full_refresh:
        os.environ["SYNC_MODE"] = "full"
    if args.start_date:
        os.environ["START_DATE"] = args.start_date
    get_settings.cache_clear()
    settings = get_settings()
    log.info(
        "ETL starting: bucket=%s prefix=%s mode=%s start_date=%s",
        settings.GCS_BUCKET or "(local)",
        settings.GCS_PREFIX,
        "full" if settings.is_full_refresh else "incremental",
        settings.START_DATE,
    )
    try:
        max_date = run_sync()
        if max_date is not None:
            log.info("Sync complete. Last partition date: %s", max_date.isoformat())
        else:
            log.info("Sync complete. No new partitions processed.")
    except Exception as e:
        log.exception("Sync failed: %s", e)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
