"""FastAPI app: lifespan DuckDB + view over Parquet, include routes."""

from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.db import connection_api, parquet_source_sql

from api.routes import health, providers, platforms, top, stats

# Empty schema matching Parquet so the API can start before ETL has run
_EMPTY_SOURCE_SQL = """
CREATE OR REPLACE TABLE source_empty (
    UNIQUE_ID VARCHAR,
    FILE_UUID VARCHAR,
    LON DOUBLE,
    LAT DOUBLE,
    DEPTH DOUBLE,
    TIME TIMESTAMP,
    PLATFORM_NAME VARCHAR,
    PROVIDER VARCHAR
);
CREATE OR REPLACE VIEW source AS SELECT * FROM source_empty;
"""


@asynccontextmanager
async def lifespan(app: FastAPI):
    con = connection_api()
    sql_expr = parquet_source_sql()
    try:
        con.execute(f"CREATE OR REPLACE VIEW source AS SELECT * FROM {sql_expr}")
    except Exception as e:
        msg = str(e).lower()
        if "no files found" in msg or "no files" in msg:
            con.execute("DROP VIEW IF EXISTS source;")
            con.execute("DROP TABLE IF EXISTS source_empty;")
            con.execute(_EMPTY_SOURCE_SQL)
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
