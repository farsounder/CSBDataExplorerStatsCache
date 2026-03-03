variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region for Cloud Run and Scheduler"
  type        = string
  default     = "us-central1"
}

variable "bucket_name" {
  description = "GCS bucket name for Parquet and state (must be globally unique)"
  type        = string
}

variable "api_image" {
  description = "Container image for the API service (e.g. us-central1-docker.pkg.dev/PROJECT/REPO/api:latest)"
  type        = string
  default     = ""
}

variable "etl_image" {
  description = "Container image for the ETL job (e.g. us-central1-docker.pkg.dev/PROJECT/REPO/etl:latest)"
  type        = string
  default     = ""
}

variable "incremental_schedule" {
  description = "Cron schedule for incremental ETL (default: daily at 02:00 UTC)"
  type        = string
  default     = "0 2 * * *"
}

variable "full_refresh_schedule" {
  description = "Cron schedule for full-refresh ETL (default: weekly Sunday 03:00 UTC). Set to empty string to disable."
  type        = string
  default     = "0 3 * * 0"
}
