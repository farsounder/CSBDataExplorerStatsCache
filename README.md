# CSB Stats API

Two-stage pipeline and API for [NOAA Crowdsourced Bathymetry (CSB)](https://noaa-dcdb-bathymetry-pds.s3.amazonaws.com/docs/readme.html): sync S3 CSV → GCS Parquet (ETL), then serve analytics via FastAPI.

## Setup

```bash
uv sync                    # create .venv and install deps (add --extra gcp for optional GCP deps)
cp .env.example .env       # set GCS_BUCKET + GCS_PREFIX (GCP) or LOCAL_STORAGE_PATH (local testing)
```

## ETL (Compressor)

Sync from `s3://noaa-dcdb-bathymetry-pds/csb/csv/` to Hive-partitioned Parquet on GCS.

- **Incremental** (default): only partition dates after last run.
- **Full refresh**: re-process all dates (catches late-arriving data).

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

Serves analytics over the Parquet store on GCS.

```bash
uv run uvicorn api.main:app --reload --port 8000
```

Endpoints:

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Liveness |
| `GET /providers` | Distinct providers |
| `GET /platforms` | Distinct UNIQUE_ID, PLATFORM_NAME |
| `GET /top-platforms?days=30&limit=10` | Top platforms by point count |
| `GET /top-providers?days=30&limit=10` | Top providers by point count |
| `GET /stats/daily/platform/{id}` | Daily point counts for a platform |
| `GET /stats/global` | Total points, files, unique platforms |
| `GET /stats/discovery-rate` | New platforms per month |

Env: `GCS_BUCKET`, `GCS_PREFIX`; optional `GCS_KEY`/`GCS_SECRET`. In GCP, default credentials are used when keys are not set.

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

## Deployment (GCP)

- **Cloud Run Job** (ETL): build from `Dockerfile.etl`; schedule with Cloud Scheduler (e.g. daily). Optional second schedule with `SYNC_MODE=full` (e.g. weekly).
- **Cloud Run Service** (API): build from `Dockerfile.api`; set `/health` as health check.
