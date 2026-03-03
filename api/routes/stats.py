"""GET /stats/daily/platform/{id}, /stats/global, /stats/discovery-rate."""

from fastapi import APIRouter, Depends, Request
from duckdb import DuckDBPyConnection

from api.queries import (
    STATS_DAILY_PLATFORM,
    STATS_GLOBAL,
    STATS_DISCOVERY_RATE,
)

router = APIRouter(prefix="/stats", tags=["stats"])


def get_db(request: Request) -> DuckDBPyConnection:
    return request.app.state.db


@router.get("/daily/platform/{id}")
def stats_daily_platform(
    id: str,
    db: DuckDBPyConnection = Depends(get_db),
) -> dict:
    result = db.execute(STATS_DAILY_PLATFORM, [id]).fetchall()
    # Plan: 200 with {"daily": []} for unknown platform
    return {
        "daily": [
            {"date": row[0].isoformat() if hasattr(row[0], "isoformat") else str(row[0]), "points": row[1]}
            for row in result
        ]
    }


@router.get("/global")
def stats_global(db: DuckDBPyConnection = Depends(get_db)) -> dict:
    row = db.execute(STATS_GLOBAL).fetchone()
    return {
        "total_points": row[0],
        "total_files": row[1],
        "unique_platforms": row[2],
    }


@router.get("/discovery-rate")
def stats_discovery_rate(db: DuckDBPyConnection = Depends(get_db)) -> dict:
    result = db.execute(STATS_DISCOVERY_RATE).fetchall()
    # Group by month: list of { unique_id, platform_name } and count
    by_month: dict[str, list[dict[str, str]]] = {}
    for row in result:
        month, unique_id, platform_name = row[0], row[1], row[2] or ""
        if month not in by_month:
            by_month[month] = []
        by_month[month].append({"unique_id": unique_id, "platform_name": platform_name})
    return {
        "by_month": [
            {
                "month": month,
                "new_platforms_count": len(platforms),
                "platforms": platforms,
            }
            for month, platforms in sorted(by_month.items())
        ]
    }
