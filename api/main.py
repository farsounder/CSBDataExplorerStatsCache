"""FastAPI app: lifespan DuckDB + view over Parquet, include routes."""

from contextlib import asynccontextmanager
from datetime import datetime, timezone
import logging
from threading import Lock
import time

from fastapi import FastAPI
from duckdb import DuckDBPyConnection

from src.config import get_settings
from src.db import connection_api, parquet_source_sql

from api.routes import health, providers, platforms, top, stats

logger = logging.getLogger(__name__)

# Empty rollup schema so the API can start before ETL has run
_EMPTY_ROLLUP_SQL = """
CREATE OR REPLACE TABLE rollup_empty (
    unique_id VARCHAR,
    platform_name VARCHAR,
    provider VARCHAR,
    day DATE,
    point_count BIGINT,
    file_count BIGINT
);
CREATE OR REPLACE VIEW rollup AS SELECT * FROM rollup_empty;
"""


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _set_empty_rollup(con: DuckDBPyConnection) -> None:
    con.execute("DROP VIEW IF EXISTS rollup;")
    con.execute("DROP TABLE IF EXISTS rollup_cache;")
    con.execute("DROP TABLE IF EXISTS rollup_empty;")
    con.execute(_EMPTY_ROLLUP_SQL)


def refresh_rollup_cache(con: DuckDBPyConnection, sql_expr: str) -> tuple[int, float]:
    start = time.perf_counter()
    con.execute(f"CREATE OR REPLACE TABLE rollup_cache AS SELECT * FROM {sql_expr}")
    con.execute("CREATE OR REPLACE VIEW rollup AS SELECT * FROM rollup_cache")
    row_count = con.execute("SELECT COUNT(*) FROM rollup_cache").fetchone()[0]
    elapsed = time.perf_counter() - start
    return row_count, elapsed


@asynccontextmanager
async def lifespan(app: FastAPI):
    con = connection_api()
    settings = get_settings()
    sql_expr = parquet_source_sql()
    app.state.db = con
    app.state.api_cache_mode = settings.API_CACHE_MODE
    app.state.api_refresh_interval_seconds = settings.API_REFRESH_INTERVAL_SECONDS
    app.state.last_materialized_at = None
    app.state.cache_refresh_lock = Lock()

    def _refresh_cache() -> datetime:
        row_count, elapsed = refresh_rollup_cache(con, sql_expr)
        refreshed_at = _utcnow()
        app.state.last_materialized_at = refreshed_at
        logger.info(
            "Materialized rollup_cache: rows=%s elapsed_s=%.3f refreshed_at=%s",
            row_count,
            elapsed,
            refreshed_at.isoformat(),
        )
        return refreshed_at

    app.state.refresh_rollup_cache = _refresh_cache

    try:
        if settings.API_CACHE_MODE == "materialize":
            _refresh_cache()
        else:
            con.execute("DROP TABLE IF EXISTS rollup_cache;")
            con.execute(f"CREATE OR REPLACE VIEW rollup AS SELECT * FROM {sql_expr}")
            logger.info("Using remote rollup view (API_CACHE_MODE=remote)")
    except Exception as e:
        msg = str(e).lower()
        if "no files found" in msg or "no files" in msg:
            _set_empty_rollup(con)
            app.state.last_materialized_at = _utcnow()
            logger.info("No rollup files found; initialized empty rollup view")
        else:
            raise
    yield
    con.close()


app = FastAPI(
    title="NOAA CSB Analytics API",
    description="Analytics over NOAA Crowdsourced Bathymetry Parquet on GCS",
    lifespan=lifespan,
)

app.include_router(health.router)
app.include_router(providers.router)
app.include_router(platforms.router)
app.include_router(top.router)
app.include_router(stats.router)
