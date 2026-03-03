#!/usr/bin/env bash
# Build and push API and ETL images to Artifact Registry.
# Tags with :latest and :<unique> so Terraform sees a new image and redeploys Cloud Run.
# Usage: ./scripts/build-push.sh PROJECT_ID [REGION]
set -euo pipefail

PROJECT_ID="${1:?Usage: $0 PROJECT_ID [REGION]}"
REGION="${2:-us-central1}"
REPO="noaa-csb-analytics"
REGISTRY="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}"

# Unique tag so Terraform apply picks up the new image (otherwise :latest doesn't change)
TAG="$(git rev-parse --short HEAD 2>/dev/null || echo "build-$(date +%Y%m%d-%H%M%S)")"

echo "Configuring Docker for ${REGISTRY}..."
gcloud auth configure-docker "${REGISTRY}" --quiet

echo "Building and pushing API (tag: ${TAG})..."
docker build -f Dockerfile.api -t "${REGISTRY}/api:latest" -t "${REGISTRY}/api:${TAG}" .
docker push "${REGISTRY}/api:latest"
docker push "${REGISTRY}/api:${TAG}"

echo "Building and pushing ETL (tag: ${TAG})..."
docker build -f Dockerfile.etl -t "${REGISTRY}/etl:latest" -t "${REGISTRY}/etl:${TAG}" .
docker push "${REGISTRY}/etl:latest"
docker push "${REGISTRY}/etl:${TAG}"

echo ""
echo "Done. Run Terraform so Cloud Run uses the new images:"
echo "  cd terraform"
echo "  terraform apply -var=\"project_id=${PROJECT_ID}\" -var=\"bucket_name=YOUR_BUCKET\" -var=\"api_image=${REGISTRY}/api:${TAG}\" -var=\"etl_image=${REGISTRY}/etl:${TAG}\""
