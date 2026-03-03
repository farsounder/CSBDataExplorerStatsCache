"""GET /top-platforms and /top-providers — by days and limit."""

from fastapi import APIRouter, Depends, Query, Request
from duckdb import DuckDBPyConnection

from api.queries import TOP_PLATFORMS, TOP_PROVIDERS

router = APIRouter(tags=["top"])


def get_db(request: Request) -> DuckDBPyConnection:
    return request.app.state.db


@router.get("/top-platforms")
def top_platforms(
    db: DuckDBPyConnection = Depends(get_db),
    days: int = Query(ge=1, le=3650),
    limit: int = Query(10, ge=1, le=1000),
) -> dict:
    result = db.execute(TOP_PLATFORMS, [days, limit]).fetchall()
    return {
        "platforms": [
            {"platform_name": row[0], "points": row[1]}
            for row in result
        ]
    }


@router.get("/top-providers")
def top_providers(
    db: DuckDBPyConnection = Depends(get_db),
    days: int = Query(ge=1, le=3650),
    limit: int = Query(10, ge=1, le=1000),
) -> dict:
    result = db.execute(TOP_PROVIDERS, [days, limit]).fetchall()
    return {
        "providers": [
            {"provider": row[0], "points": row[1]}
            for row in result
        ]
    }
