"""ETL entrypoint: load config (env + CLI), run sync, exit 0 on success."""

from __future__ import annotations

import argparse
import os
import sys

from src.config import get_settings

from etl.sync import run_sync


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
    # Reload settings after env override
    get_settings.cache_clear()
    try:
        max_date = run_sync()
        if max_date is not None:
            print(f"Sync complete. Last partition date: {max_date.isoformat()}")
        else:
            print("Sync complete. No new partitions processed.")
    except Exception as e:
        print(f"Sync failed: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
