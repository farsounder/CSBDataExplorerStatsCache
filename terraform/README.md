# GCP infrastructure (Terraform)

This directory provisions everything needed for NOAA CSB Analytics on GCP from the command line: no console required.

## What gets created

- **APIs**: Cloud Run, Cloud Storage, Cloud Scheduler, Artifact Registry
- **GCS bucket**: Parquet and ETL state
- **Artifact Registry repo**: For your API and ETL container images
- **Service account** (`noaa-csb-run`): Used by Cloud Run and Scheduler; has read/write on the bucket and permission to run the ETL job
- **Cloud Run Service** (API): Serves the FastAPI app
- **Cloud Run Job** (ETL): Runs the Compressor; same image, env `SYNC_MODE=incremental` by default
- **Cloud Scheduler** (incremental): Triggers the ETL job daily (default 02:00 UTC)
- **Cloud Scheduler** (full refresh, optional): Triggers the ETL job with `SYNC_MODE=full` weekly (default Sunday 03:00 UTC)

## Prerequisites

- [Terraform](https://developer.hashicorp.com/terraform/install) >= 1.5
- [Google Cloud SDK (gcloud)](https://cloud.google.com/sdk/docs/install)
- A GCP project (create one in the console or with `gcloud projects create PROJECT_ID`)

## One-time setup

```bash
# 1. Log in and set the project
gcloud auth application-default login
gcloud config set project YOUR_PROJECT_ID

# 2. Ensure the project has a billing account (required for Run/Scheduler)
# Link in console: Billing → Link a billing account
```

## Apply infrastructure

```bash
cd terraform

# 3. Initialize Terraform
terraform init

# 4. Plan (bucket name must be globally unique)
terraform plan \
  -var="project_id=YOUR_PROJECT_ID" \
  -var="bucket_name=YOUR_UNIQUE_BUCKET_NAME"

# 5. Apply (creates all resources; uses placeholder images at first)
terraform apply \
  -var="project_id=YOUR_PROJECT_ID" \
  -var="bucket_name=YOUR_UNIQUE_BUCKET_NAME"
```

After the first apply you’ll have a bucket, Artifact Registry repo, Cloud Run service and job (using a placeholder image), and Scheduler jobs. The API URL is in the output.

## Build and deploy your images

**Option A — Local (recommended first):** From repo root, run the helper script.

```powershell
# Windows (PowerShell, from repo root)
.\scripts\build-push.ps1 YOUR_PROJECT_ID
.\scripts\build-push.ps1 YOUR_PROJECT_ID -Region europe-west1
```

```bash
# macOS/Linux
./scripts/build-push.sh YOUR_PROJECT_ID
REGION=europe-west1 ./scripts/build-push.sh YOUR_PROJECT_ID
```

**Option B — Manual commands:** Configure Docker for Artifact Registry, then build and push:

```bash
# From repo root
REGION=us-central1
PROJECT=YOUR_PROJECT_ID
REPO=noaa-csb-analytics

gcloud auth configure-docker ${REGION}-docker.pkg.dev

docker build -f Dockerfile.api -t ${REGION}-docker.pkg.dev/${PROJECT}/${REPO}/api:latest .
docker push ${REGION}-docker.pkg.dev/${PROJECT}/${REPO}/api:latest

docker build -f Dockerfile.etl -t ${REGION}-docker.pkg.dev/${PROJECT}/${REPO}/etl:latest .
docker push ${REGION}-docker.pkg.dev/${PROJECT}/${REPO}/etl:latest
```

**Option C — GitHub Actions:** After it works locally, add secrets `GCP_PROJECT_ID` and `GCP_SA_KEY` and push to `main`; see the main repo [README](../README.md#build-and-push-images).

Point Terraform at your images and re-apply. **Use a unique tag** (e.g. the one the script prints) so Terraform sees a change and redeploys; re-pushing `:latest` alone doesn’t change the image URL, so apply would report no changes.

```powershell
cd terraform
terraform apply -var="project_id=YOUR_PROJECT_ID" -var="bucket_name=YOUR_BUCKET" -var="api_image=.../api:TAG" -var="etl_image=.../etl:TAG"
```

The build script tags each image with both `:latest` and a unique tag (git short SHA or timestamp) and prints the exact `terraform apply` line to run. Your API URL (from `terraform output api_url`) will then serve the real app.

## Optional variables

- **region** (default `us-central1`): Where Run, Scheduler, and the bucket live.
- **api_cache_mode** (default `materialize`): `materialize` loads rollup once at startup into local DuckDB for faster requests; `remote` reads Parquet directly per query.
- **api_refresh_interval_seconds** (default `86400`): In `materialize` mode, cache refresh interval for active API instances.
- **api_max_instance_request_concurrency** (default `1`): Max concurrent requests per API instance. Keep `1` when sharing one DuckDB connection in-process.
- **incremental_schedule** (default `0 2 * * `*): Cron for daily incremental ETL (02:00 UTC).
- **full_refresh_schedule** (default `0 3 * * 0`): Cron for weekly full refresh (Sunday 03:00 UTC). Set to `""` to disable.
- **api_image** / **etl_image**: Leave empty on first apply to use a placeholder; set to your image URLs after push.

## Run ETL manually

```bash
gcloud run jobs execute noaa-csb-etl --region=us-central1
# Full refresh:
gcloud run jobs execute noaa-csb-etl --region=us-central1 --update-env-vars=SYNC_MODE=full
```

## Outputs

- `bucket_name`: Use as `GCS_BUCKET` in app config.
- `api_url`: Public URL of the API.
- `artifact_registry_repo`: Repository for `docker push`.
- `service_account_email`: SA used by Run (e.g. for local auth with workload identity).

## Destroy

```bash
terraform destroy -var="project_id=YOUR_PROJECT_ID" -var="bucket_name=YOUR_UNIQUE_BUCKET_NAME"
```

This removes the bucket (if empty or with `force_destroy`), Run service/job, Scheduler jobs, and related IAM. The Artifact Registry repo and any pushed images remain unless you delete them separately.