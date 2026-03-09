"""GET /platforms and /platforms/{id} platform metadata lookups."""

from fastapi import APIRouter, Depends, HTTPException
from duckdb import DuckDBPyConnection

from api.deps import get_db
from api.queries import PLATFORM_BY_ID, PLATFORMS

router = APIRouter(tags=["platforms"])


@router.get("/platforms")
def platforms(db: DuckDBPyConnection = Depends(get_db)) -> dict:
    """List all distinct platforms with unique ID, display name, and provider.

    Use this endpoint to discover valid platform identifiers for stats queries.
    """
    result = db.execute(PLATFORMS).fetchall()
    return {
        "platforms": [
            {"unique_id": row[0], "platform_name": row[1], "provider": row[2]}
            for row in result
        ]
    }


@router.get("/platforms/{id}")
def platform_by_id(id: str, db: DuckDBPyConnection = Depends(get_db)) -> dict:
    """Return one platform row by unique ID, including provider metadata."""
    row = db.execute(PLATFORM_BY_ID, [id]).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Platform not found")
    return {"unique_id": row[0], "platform_name": row[1], "provider": row[2]}
