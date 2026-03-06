"""Stats endpoints for daily trends, totals, and discovery rate."""

from fastapi import APIRouter, Depends, Query, Request
from duckdb import DuckDBPyConnection

from api.queries import (
    STATS_DAILY_ALL_PLATFORMS,
    STATS_DAILY_ALL_PROVIDERS,
    STATS_DAILY_PLATFORM,
    STATS_DAILY_PROVIDER,
    STATS_GLOBAL,
    STATS_DISCOVERY_RATE,
)

router = APIRouter(prefix="/stats", tags=["stats"])


def get_db(request: Request) -> DuckDBPyConnection:
    return request.app.state.db


def _date_to_str(value: object) -> str:
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


@router.get("/daily/platform/{id}")
def stats_daily_platform(
    id: str,
    days: int = Query(30, ge=1, le=3650),
    db: DuckDBPyConnection = Depends(get_db),
) -> dict:
    """Get daily point totals for one platform across the trailing `days` window.

    Returns an empty `daily` list when no data exists for the provided platform ID.
    """
    result = db.execute(STATS_DAILY_PLATFORM, [id, days]).fetchall()
    # Plan: 200 with {"daily": []} for unknown platform
    return {
        "daily": [
            {"date": _date_to_str(row[0]), "points": row[1]}
            for row in result
        ]
    }


@router.get("/daily/provider/{provider}")
def stats_daily_provider(
    provider: str,
    days: int = Query(30, ge=1, le=3650),
    db: DuckDBPyConnection = Depends(get_db),
) -> dict:
    """Get daily point totals for one provider across the trailing `days` window.

    The provider value is matched exactly as stored in the rollup dataset.
    """
    result = db.execute(STATS_DAILY_PROVIDER, [provider, days]).fetchall()
    return {
        "daily": [
            {"date": _date_to_str(row[0]), "points": row[1]}
            for row in result
        ]
    }


@router.get("/daily/all/providers")
def stats_daily_all_providers(
    days: int = Query(30, ge=1, le=3650),
    db: DuckDBPyConnection = Depends(get_db),
) -> dict:
    """Get daily point totals for all providers within the trailing `days` window.

    Each item includes date, provider, and aggregated point count for that day.
    """
    result = db.execute(STATS_DAILY_ALL_PROVIDERS, [days]).fetchall()
    return {
        "daily": [
            {"date": _date_to_str(row[0]), "provider": row[1], "points": row[2]}
            for row in result
        ]
    }


@router.get("/daily/all/platforms")
def stats_daily_all_platforms(
    days: int = Query(30, ge=1, le=3650),
    db: DuckDBPyConnection = Depends(get_db),
) -> dict:
    """Get daily point totals for all platforms within the trailing `days` window.

    Each item includes date, platform identifiers, and aggregated point count.
    """
    result = db.execute(STATS_DAILY_ALL_PLATFORMS, [days]).fetchall()
    return {
        "daily": [
            {
                "date": _date_to_str(row[0]),
                "unique_id": row[1],
                "platform_name": row[2],
                "points": row[3],
            }
            for row in result
        ]
    }


@router.get("/global")
def stats_global(db: DuckDBPyConnection = Depends(get_db)) -> dict:
    """Return high-level aggregate metrics across the full available dataset."""
    row = db.execute(STATS_GLOBAL).fetchone()
    return {
        "total_points": row[0] or 0,
        "total_files": row[1] or 0,
        "unique_platforms": row[2] or 0,
    }


@router.get("/discovery-rate")
def stats_discovery_rate(db: DuckDBPyConnection = Depends(get_db)) -> dict:
    """Return monthly counts and lists of newly observed platforms.

    Useful for tracking platform discovery trends over time.
    """
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
