"""DuckDB connection factory: httpfs + GCS for ETL; GCS-only for API."""

from __future__ import annotations

import duckdb
from duckdb import DuckDBPyConnection

from src.config import get_settings


def _configure_httpfs_s3(con: DuckDBPyConnection) -> None:
    """Enable httpfs and configure S3 for public NOAA bucket (read-only)."""
    con.execute("INSTALL httpfs; LOAD httpfs;")
    settings = get_settings()
    con.execute(f"SET s3_region='{settings.S3_REGION}';")
    con.execute("SET s3_url_style='path';")
    # Public bucket: no credentials needed
    con.execute("SET s3_access_key_id='';")
    con.execute("SET s3_secret_access_key='';")


def _configure_gcs(con: DuckDBPyConnection) -> None:
    """Configure GCS: key/secret from env, or GCP credential_chain when on GCP. No-op when using local storage."""
    settings = get_settings()
    if settings.is_local_storage:
        return
    if settings.GCS_KEY and settings.GCS_SECRET:
        con.execute(f"SET google_cloud_storage_access_key_id='{settings.GCS_KEY}';")
        con.execute(f"SET google_cloud_storage_secret_access_key='{settings.GCS_SECRET}';")
    else:
        try:
            con.execute("CREATE SECRET (TYPE gcs, PROVIDER credential_chain);")
        except Exception:
            pass


def connection_etl() -> DuckDBPyConnection:
    """DuckDB connection for ETL: S3 read (httpfs) + GCS or local write."""
    con = duckdb.connect()
    _configure_httpfs_s3(con)
    _configure_gcs(con)
    return con


def connection_api() -> DuckDBPyConnection:
    """DuckDB connection for API: GCS or local read-only (Parquet)."""
    con = duckdb.connect()
    if not get_settings().is_local_storage:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        _configure_gcs(con)
    return con


def parquet_source_sql() -> str:
    """SQL expression for the Parquet source (Hive-partitioned). Use in FROM clause."""
    settings = get_settings()
    base = settings.storage_base_uri
    # Local: path with forward slashes; GCS: gs://bucket/prefix
    return f"read_parquet('{base}/**/*.parquet', hive_partitioning=true)"
