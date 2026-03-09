"""GET /top-platforms and /top-providers — by days and limit."""

from fastapi import APIRouter, Depends, Query
from duckdb import DuckDBPyConnection

from api.deps import get_db
from api.queries import TOP_PLATFORMS, TOP_PROVIDERS

router = APIRouter(tags=["top"])


@router.get("/top-platforms")
def top_platforms(
    db: DuckDBPyConnection = Depends(get_db),
    days: int = Query(ge=1, le=3650),
    limit: int = Query(10, ge=1, le=1000),
) -> dict:
    """Return top platforms by unique ID over a trailing time window.

    The `days` parameter defines the lookback period and `limit` caps returned rows.
    """
    result = db.execute(TOP_PLATFORMS, [days, limit]).fetchall()
    return {
        "platforms": [
            {"unique_id": row[0], "platform_name": row[1], "points": row[2]}
            for row in result
        ]
    }


@router.get("/top-providers")
def top_providers(
    db: DuckDBPyConnection = Depends(get_db),
    days: int = Query(ge=1, le=3650),
    limit: int = Query(10, ge=1, le=1000),
) -> dict:
    """Return top providers by point volume over a trailing time window.

    The `days` parameter defines the lookback period and `limit` caps returned rows.
    """
    result = db.execute(TOP_PROVIDERS, [days, limit]).fetchall()
    return {
        "providers": [
            {"provider": row[0], "points": row[1]}
            for row in result
        ]
    }
