"""FastAPI app: lifespan DuckDB + view over Parquet, include routes."""

from contextlib import asynccontextmanager
import logging
import time

from fastapi import FastAPI

from src.db import connection_api, parquet_source_sql
from src.config import get_settings

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


@asynccontextmanager
async def lifespan(app: FastAPI):
    con = connection_api()
    settings = get_settings()
    sql_expr = parquet_source_sql()
    try:
        if settings.API_CACHE_MODE == "materialize":
            start = time.perf_counter()
            con.execute(f"CREATE OR REPLACE TABLE rollup_cache AS SELECT * FROM {sql_expr}")
            con.execute("CREATE OR REPLACE VIEW rollup AS SELECT * FROM rollup_cache")
            elapsed = time.perf_counter() - start
            row_count = con.execute("SELECT COUNT(*) FROM rollup_cache").fetchone()[0]
            logger.info(
                "Materialized rollup_cache at startup: rows=%s elapsed_s=%.3f",
                row_count,
                elapsed,
            )
        else:
            con.execute("DROP TABLE IF EXISTS rollup_cache;")
            con.execute(f"CREATE OR REPLACE VIEW rollup AS SELECT * FROM {sql_expr}")
            logger.info("Using remote rollup view (API_CACHE_MODE=remote)")
    except Exception as e:
        msg = str(e).lower()
        if "no files found" in msg or "no files" in msg:
            con.execute("DROP VIEW IF EXISTS rollup;")
            con.execute("DROP TABLE IF EXISTS rollup_cache;")
            con.execute("DROP TABLE IF EXISTS rollup_empty;")
            con.execute(_EMPTY_ROLLUP_SQL)
        else:
            raise
    app.state.db = con
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
