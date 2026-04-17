# CSB Stats API

Two-stage pipeline and API for [NOAA Crowdsourced Bathymetry (CSB)](https://noaa-dcdb-bathymetry-pds.s3.amazonaws.com/docs/readme.html): sync S3 CSV → GCS Parquet (ETL), then serve analytics via FastAPI.

## Setup

```bash
uv sync                    # create .venv and install deps (add --extra gcp for optional GCP deps)
cp .env.example .env       # set GCS_BUCKET + GCS_PREFIX (GCP) or LOCAL_STORAGE_PATH (local testing)
```

## ETL (Rollup builder)

Reads NOAA CSVs from `s3://noaa-dcdb-bathymetry-pds/csb/csv/` using Python CSV parsing and writes monthly rollup Parquet with PyArrow.
DuckDB is intentionally not used in ETL ingest/write.

ETL output:

- `rollup/year=YYYY/month=MM/data.parquet`
- `_state/last_sync.json`
- `_state/failed_csvs/day=YYYY-MM-DD.json` (only when files are skipped)

Rollup schema:

- `unique_id`
- `platform_name`
- `provider`
- `day`
- `point_count`
- `file_count`

- **Incremental** (default): re-process the last synced day and then continue through yesterday.
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

### ETL runtime behavior

- **Start date precedence (incremental):** `last_sync_date` from state (inclusive, to catch late-arriving files) → `START_DATE` → earliest date discovered in NOAA bucket.
- **Checkpointing:** state is updated **after each successful month partition write**, using the max processed source date for that year/month.
- **Resume safety:** if a run fails mid-way, previously written months are already checkpointed and do not need to be reprocessed.
- **Bad timestamps:** rows with unparseable `TIME` are skipped from rollup and logged.
- **Bad files:** unreadable/problematic CSV files are skipped and logged to `_state/failed_csvs/` so the run can continue.
- **Incremental merge semantics:** for overlapping `(unique_id, platform_name, provider, day)` keys, the new run replaces prior values for those keys (idempotent re-run behavior).

## API

Serves analytics from the rollup Parquet only (`{storage_base}/rollup/**/*.parquet`) using DuckDB in the API process.

```bash
uv run uvicorn api.main:app --reload --port 8000
```

Endpoints:

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Liveness |
| `GET /providers` | Distinct providers |
| `GET /platforms` | Distinct UNIQUE_ID, PLATFORM_NAME, PROVIDER |
| `GET /platforms/{id}` | Platform metadata by UNIQUE_ID |
| `GET /top-platforms?days=30&limit=10` | Top platforms by point count |
| `GET /top-providers?days=30&limit=10` | Top providers by point count |
| `GET /stats/daily/platform/{id}?days=30` | Daily point counts for a platform (lookback window) |
| `GET /stats/daily/provider/{provider}?days=30` | Daily point counts for a provider (lookback window) |
| `GET /stats/daily/all/providers?days=30` | Daily point counts split by provider (stacked chart friendly) |
| `GET /stats/daily/all/platforms?days=30` | Daily point counts split by platform (stacked chart friendly) |
| `GET /stats/global` | Total points, files, unique platforms |
| `GET /stats/discovery-rate` | New platforms per month |

Env: `GCS_BUCKET`, `GCS_PREFIX`; optional `GCS_KEY`/`GCS_SECRET`, `API_CACHE_MODE=remote|materialize` (default `remote`), `API_REFRESH_INTERVAL_SECONDS` (default `86400`, used in materialize mode). In GCP, default credentials are used when keys are not set.

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

### Recovery workflow after ETL failure

1. Check ETL logs for the last successfully written year/month partition and checkpoint message.
2. Confirm `_state/last_sync.json` advanced to that completed source date.
3. Re-run incremental ETL; it resumes from the next date and rewrites only touched month partitions.

## Docker (single image)

- **ETL**: `docker build -f Dockerfile.etl -t csb-etl . && docker run --env-file .env csb-etl`
- **API**: `docker build -f Dockerfile.api -t csb-api . && docker run -p 8080:8080 --env-file .env csb-api`

## Deployment (GCP)

Infrastructure as code: use Terraform so you don’t need the console.

- **[terraform/README.md](terraform/README.md)** — Create project, enable APIs, GCS bucket, Artifact Registry, Cloud Run (API + ETL job), and Cloud Scheduler from the CLI. Then build/push your images and re-apply.
- **Cloud Run Service** (API): build from `Dockerfile.api`; health check `/health`.
- **Cloud Run Job** (ETL): build from `Dockerfile.etl`; Scheduler runs it daily (incremental) and optionally weekly (full refresh).

### Build and push images

**Recommended: try locally first**, then automate with the script or GitHub Action once it works.

**Local (one-time auth, then build/push):**

From repo root in **PowerShell** (or cmd with `set REGION=...` and run the `docker` lines):

```powershell
.\scripts\build-push.ps1 YOUR_PROJECT_ID
# Optional different region: .\scripts\build-push.ps1 YOUR_PROJECT_ID -Region europe-west1
```

Or run the steps manually:

```powershell
$REGION = "us-central1"
$PROJECT = "YOUR_PROJECT_ID"
$REPO = "noaa-csb-analytics"
gcloud auth configure-docker ${REGION}-docker.pkg.dev
docker build -f Dockerfile.api -t "${REGION}-docker.pkg.dev/${PROJECT}/${REPO}/api:latest" .
docker push "${REGION}-docker.pkg.dev/${PROJECT}/${REPO}/api:latest"
docker build -f Dockerfile.etl -t "${REGION}-docker.pkg.dev/${PROJECT}/${REPO}/etl:latest" .
docker push "${REGION}-docker.pkg.dev/${PROJECT}/${REPO}/etl:latest"
```

The script tags images with both `:latest` and a unique tag (git SHA or timestamp) and prints the exact `terraform apply` command to run—use that so Terraform sees a new image and redeploys (re-pushing `:latest` alone doesn’t change the URL, so apply would show no changes). See [terraform/README.md](terraform/README.md).

*(On macOS/Linux: `./scripts/build-push.sh YOUR_PROJECT_ID`.)*

**GitHub Actions:** On push to `main`, the workflow in [.github/workflows/build-push.yml](.github/workflows/build-push.yml) can build and push both images to Artifact Registry. Add repo secrets `GCP_PROJECT_ID` and `GCP_SA_KEY` (JSON key for a SA with `roles/artifactregistry.writer`), then enable the workflow when you're ready.
