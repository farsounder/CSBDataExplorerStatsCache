# Build and push API and ETL images to Artifact Registry.
# Tags with :latest and :<unique> so Terraform sees a new image and redeploys Cloud Run.
# Usage: .\scripts\build-push.ps1 PROJECT_ID [REGION]
#   REGION defaults to us-central1.
param(
    [Parameter(Mandatory = $true)]
    [string]$ProjectId,
    [string]$Region = "us-central1"
)

$ErrorActionPreference = "Stop"
$Repo = "noaa-csb-analytics"
$Registry = "$Region-docker.pkg.dev/$ProjectId/$Repo"

# Unique tag so Terraform apply picks up the new image (otherwise :latest doesn't change)
try {
    $Tag = (git rev-parse --short HEAD 2>$null)
} catch {}
if (-not $Tag) { $Tag = "build-$(Get-Date -Format 'yyyyMMdd-HHmmss')" }

Write-Host "Configuring Docker for $Registry..."
gcloud auth configure-docker $Registry --quiet

Write-Host "Building and pushing API (tag: $Tag)..."
docker build -f Dockerfile.api -t "${Registry}/api:latest" -t "${Registry}/api:${Tag}" .
docker push "${Registry}/api:latest"
docker push "${Registry}/api:${Tag}"

Write-Host "Building and pushing ETL (tag: $Tag)..."
docker build -f Dockerfile.etl -t "${Registry}/etl:latest" -t "${Registry}/etl:${Tag}" .
docker push "${Registry}/etl:latest"
docker push "${Registry}/etl:${Tag}"

Write-Host ""
Write-Host "Done. Run Terraform so Cloud Run uses the new images:"
Write-Host "  cd terraform"
Write-Host "  terraform apply -var=`"project_id=$ProjectId`" -var=`"bucket_name=YOUR_BUCKET`" -var=`"api_image=$Registry/api:$Tag`" -var=`"etl_image=$Registry/etl:$Tag`""
