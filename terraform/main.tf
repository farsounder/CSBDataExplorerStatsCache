# Enable required APIs
resource "google_project_service" "run" {
  project            = var.project_id
  service            = "run.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "storage" {
  project            = var.project_id
  service            = "storage.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "scheduler" {
  project            = var.project_id
  service            = "cloudscheduler.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "artifactregistry" {
  project            = var.project_id
  service            = "artifactregistry.googleapis.com"
  disable_on_destroy = false
}

# GCS bucket for Parquet and state
resource "google_storage_bucket" "data" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = false
  depends_on    = [google_project_service.storage]
}

# Artifact Registry repo for container images
resource "google_artifact_registry_repository" "repo" {
  location      = var.region
  repository_id = "noaa-csb-analytics"
  description   = "Container images for NOAA CSB API and ETL"
  format        = "DOCKER"
  depends_on    = [google_project_service.artifactregistry]
}

# Service account used by Cloud Run (API and Job) and by Scheduler to invoke the job
resource "google_service_account" "run" {
  account_id   = "noaa-csb-run"
  display_name = "NOAA CSB Analytics Cloud Run"
}

# Allow the Run SA to read/write the data bucket (API and ETL)
resource "google_storage_bucket_iam_member" "run_data" {
  bucket = google_storage_bucket.data.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.run.email}"
}

# Default placeholder image (Cloud Run hello) so Terraform can create resources before you push your own
locals {
  api_image = var.api_image != "" ? var.api_image : "us-docker.pkg.dev/cloudrun/container/hello"
  etl_image = var.etl_image != "" ? var.etl_image : "us-docker.pkg.dev/cloudrun/container/hello"
}

# Cloud Run Service (API)
resource "google_cloud_run_v2_service" "api" {
  name     = "noaa-csb-api"
  location = var.region
  ingress  = "INGRESS_TRAFFIC_ALL"

  template {
    service_account = google_service_account.run.email
    containers {
      image = local.api_image
      env {
        name  = "GCS_BUCKET"
        value = google_storage_bucket.data.name
      }
      env {
        name  = "GCS_PREFIX"
        value = "csb_analytics"
      }
      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
      }
      startup_probe {
        http_get {
          path = "/health"
        }
        initial_delay_seconds = 5
        timeout_seconds        = 3
        period_seconds         = 5
        failure_threshold      = 2
      }
      liveness_probe {
        http_get {
          path = "/health"
        }
        initial_delay_seconds = 10
        period_seconds         = 10
      }
    }
  }
  depends_on = [google_project_service.run]
}

# Allow unauthenticated invocations so the API is publicly reachable
resource "google_cloud_run_v2_service_iam_member" "api_public" {
  location = google_cloud_run_v2_service.api.location
  name     = google_cloud_run_v2_service.api.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}

# Cloud Run Job (ETL)
resource "google_cloud_run_v2_job" "etl" {
  name     = "noaa-csb-etl"
  location = var.region

  template {
    task_count = 1
      template {
      service_account = google_service_account.run.email
      containers {
        name  = "etl"
        image = local.etl_image
        env {
          name  = "GCS_BUCKET"
          value = google_storage_bucket.data.name
        }
        env {
          name  = "GCS_PREFIX"
          value = "csb_analytics"
        }
        env {
          name  = "SYNC_MODE"
          value = "incremental"
        }
        resources {
          limits = {
            cpu    = "1"
            memory = "2Gi"
          }
        }
      }
      timeout               = "3600s"
    }
  }
  depends_on = [google_project_service.run, google_storage_bucket_iam_member.run_data]
}

# Scheduler needs a SA with permission to invoke the Cloud Run Job
resource "google_cloud_run_v2_job_iam_member" "scheduler_invoker" {
  location = google_cloud_run_v2_job.etl.location
  name     = google_cloud_run_v2_job.etl.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.run.email}"
}

# Cloud Scheduler: incremental ETL (daily)
resource "google_cloud_scheduler_job" "etl_incremental" {
  name        = "noaa-csb-etl-incremental"
  description = "Run NOAA CSB ETL in incremental mode daily"
  schedule    = var.incremental_schedule
  time_zone   = "UTC"
  region      = var.region

  http_target {
    uri         = "https://run.googleapis.com/v2/projects/${var.project_id}/locations/${var.region}/jobs/${google_cloud_run_v2_job.etl.name}:run"
    http_method = "POST"
    body        = base64encode("{}")
    headers = {
      "Content-Type" = "application/json"
    }
    oidc_token {
      service_account_email = google_service_account.run.email
      audience              = "https://run.googleapis.com"
    }
  }
  depends_on = [google_project_service.scheduler, google_cloud_run_v2_job_iam_member.scheduler_invoker]
}

# Cloud Scheduler: full refresh (weekly, optional)
resource "google_cloud_scheduler_job" "etl_full_refresh" {
  count       = var.full_refresh_schedule != "" ? 1 : 0
  name        = "noaa-csb-etl-full-refresh"
  description = "Run NOAA CSB ETL in full-refresh mode weekly"
  schedule    = var.full_refresh_schedule
  time_zone   = "UTC"
  region      = var.region

  http_target {
    uri         = "https://run.googleapis.com/v2/projects/${var.project_id}/locations/${var.region}/jobs/${google_cloud_run_v2_job.etl.name}:run"
    http_method = "POST"
    body        = base64encode("{\"overrides\":{\"containerOverrides\":[{\"name\":\"etl\",\"env\":[{\"name\":\"SYNC_MODE\",\"value\":\"full\"}]}]}}")
    headers = {
      "Content-Type" = "application/json"
    }
    oidc_token {
      service_account_email = google_service_account.run.email
      audience              = "https://run.googleapis.com"
    }
  }
  depends_on = [google_project_service.scheduler, google_cloud_run_v2_job_iam_member.scheduler_invoker]
}
