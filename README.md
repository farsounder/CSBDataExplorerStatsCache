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
