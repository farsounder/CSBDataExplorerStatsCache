output "bucket_name" {
  description = "GCS bucket for Parquet and state"
  value       = google_storage_bucket.data.name
}

output "api_url" {
  description = "URL of the deployed API"
  value       = google_cloud_run_v2_service.api.uri
}

output "artifact_registry_repo" {
  description = "Artifact Registry repository for pushing images"
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.repo.repository_id}"
}

output "service_account_email" {
  description = "Service account used by Cloud Run (for local auth or CI)"
  value       = google_service_account.run.email
}
