"""GET /providers — distinct providers."""

from fastapi import APIRouter, Depends, Request
from duckdb import DuckDBPyConnection

from api.queries import PROVIDERS

router = APIRouter(tags=["providers"])


def get_db(request: Request) -> DuckDBPyConnection:
    return request.app.state.db


@router.get("/providers")
def providers(db: DuckDBPyConnection = Depends(get_db)) -> dict:
    result = db.execute(PROVIDERS).fetchall()
    return {"providers": [row[0] for row in result]}