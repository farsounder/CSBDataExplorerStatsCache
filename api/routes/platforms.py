"""GET /platforms — distinct UNIQUE_ID, PLATFORM_NAME."""

from fastapi import APIRouter, Depends, Request
from duckdb import DuckDBPyConnection

from api.queries import PLATFORMS

router = APIRouter(tags=["platforms"])


def get_db(request: Request) -> DuckDBPyConnection:
    return request.app.state.db


@router.get("/platforms")
def platforms(db: DuckDBPyConnection = Depends(get_db)) -> dict:
    result = db.execute(PLATFORMS).fetchall()
    return {
        "platforms": [
            {"unique_id": row[0], "platform_name": row[1]}
            for row in result
        ]
    }
