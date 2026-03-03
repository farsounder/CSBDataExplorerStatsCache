"""FastAPI app: lifespan DuckDB + view over Parquet, include routes."""

from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.db import connection_api, parquet_source_sql

from api.routes import health, providers, platforms, top, stats


@asynccontextmanager
async def lifespan(app: FastAPI):
    con = connection_api()
    sql_expr = parquet_source_sql()
    con.execute(f"CREATE OR REPLACE VIEW source AS SELECT * FROM {sql_expr}")
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
