"""GET /health — no DB; for Cloud Run probe."""

from fastapi import APIRouter

router = APIRouter(tags=["health"])


@router.get("/health")
def health() -> dict[str, str]:
    """Return API liveness status for health checks and uptime monitors."""
    return {"status": "ok"}
