# CSB Stats API

Two-stage pipeline and API for [NOAA Crowdsourced Bathymetry (CSB)](https://noaa-dcdb-bathymetry-pds.s3.amazonaws.com/docs/readme.html): sync S3 CSV → GCS Parquet (ETL), then serve analytics via FastAPI.

## Setup

```bash
uv sync                    # create .venv and install deps (add --extra gcp for optional GCP deps)
cp .env.example .env       # set GCS_BUCKET + GCS_PREFIX (GCP) or LOCAL_STORAGE_PATH (local testing)
```

## ETL (Rollup builder)

Reads NOAA CSVs from `s3://noaa-dcdb-bathymetry-pds/csb/csv/` using Python CSV parsing and writes monthly rollup Parquet with PyArrow.

ETL output:

- `rollup/year=YYYY/month=MM/data.parquet`
- `_state/last_sync.json`
- `_state/processed_files/day=YYYY-MM-DD.json` (per-source-day registry of S3 keys already ingested)
- `_state/failed_csvs/day=YYYY-MM-DD.json` (only when files are skipped)

Rollup schema:

- `unique_id`
- `platform_name`
- `provider`
- `day`
- `point_count`
- `file_count`
- **Incremental** (default): re-scan from the last synced source day through yesterday, but only ingest CSV keys not already listed in `_state/processed_files/`. Newly seen files are added into the existing rollup partitions.
- **Full refresh**: re-process every source date. The first time the run touches a logical month it overwrites that partition; subsequent touches add to it. This rebuilds the rollup correctly even when source files contribute to multiple logical months.

```bash
# Incremental (uses state on GCS)
uv run python -m etl.main

# Full refresh
uv run python -m etl.main --full-refresh

# First run: optional start date when no state exists
uv run python -m etl.main --start-date 2026-01-01
```

Env: `GCS_BUCKET`, `GCS_PREFIX`; optional `SYNC_MODE=incremental|full`, `START_DATE`, `GCS_KEY`/`GCS_SECRET`.

## API

Serves analytics from the rollup Parquet only (`{storage_base}/rollup/**/*.parquet`) using DuckDB in the API process.

```bash
uv run uvicorn api.main:app --reload --port 8000
```

## Test locally (no GCP)

Use **local file storage** so the API and ETL read/write under a directory instead of GCS.

**Option A: uv (host)**

```bash
uv sync
echo "LOCAL_STORAGE_PATH=./data" > .env
# 1) Sync starting on given day from NOAA S3 into ./data
uv run python -m etl.main --start-date 2026-01-01
# 2) Start API (reads from ./data)
uv run uvicorn api.main:app --reload --port 8000
# Open http://localhost:8000/health and http://localhost:8000/stats/global
```

**Option B: Docker Compose**

Compose starts the API and mounts `./data`; run the ETL once to populate that directory.

```bash
# Start API (no data yet)
docker compose up -d
# Populate data: sync one day from NOAA S3 into the shared volume
docker compose run --rm etl python -m etl.main --start-date 2026-01-01
# Check API
curl http://localhost:8080/health
curl http://localhost:8080/stats/global
```

To run ETL with more data: `docker compose run --rm etl python -m etl.main --full-refresh` (slow) or use `--start-date` / incremental. Data is persisted in `./data`.

## Docker (single image)

- **ETL**: `docker build -f Dockerfile.etl -t csb-etl . && docker run --env-file .env csb-etl`
- **API**: `docker build -f Dockerfile.api -t csb-api . && docker run -p 8080:8080 --env-file .env csb-api`
