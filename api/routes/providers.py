"""GET /providers — distinct providers."""

from fastapi import APIRouter, Depends
from duckdb import DuckDBPyConnection

from api.deps import get_db
from api.queries import PROVIDERS

router = APIRouter(tags=["providers"])


@router.get("/providers")
def providers(db: DuckDBPyConnection = Depends(get_db)) -> dict:
    """List all distinct data providers currently present in analytics rollups.

    Returns provider names in ascending order for populating filters and dashboards.
    """
    result = db.execute(PROVIDERS).fetchall()
    return {"providers": [row[0] for row in result]}