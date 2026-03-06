"""Pydantic Settings for ETL and API: bucket, prefix, sync mode, credentials."""

from __future__ import annotations

import os
from functools import lru_cache
from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Single source of truth for bucket, S3 path, GCS prefix, and credentials."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Storage: either GCS or local path for testing
    LOCAL_STORAGE_PATH: str | None = Field(
        default=None,
        description="Local directory for Parquet and state (no GCP). Set for local/Docker testing.",
    )
    GCS_BUCKET: str | None = Field(default=None, description="GCS bucket for Parquet output and state (required if not LOCAL_STORAGE_PATH)")
    GCS_PREFIX: str = Field(default="csb_analytics", description="Prefix under bucket/path for data and state")
    GCS_KEY: str | None = Field(default=None, description="GCS HMAC key ID (optional; use default credentials in GCP)")
    GCS_SECRET: str | None = Field(default=None, description="GCS HMAC secret (optional)")

    # S3 (NOAA public bucket)
    S3_BUCKET: str = Field(default="noaa-dcdb-bathymetry-pds", description="NOAA CSB S3 bucket")
    S3_PREFIX: str = Field(default="csb/csv", description="Prefix under S3 bucket for CSV files (YYYY/MM/DD/)")
    S3_REGION: str = Field(default="us-east-1", description="S3 region for NOAA bucket")

    # ETL-only
    SYNC_MODE: Literal["incremental", "full"] = Field(
        default="incremental",
        description="incremental = dates after last_sync_date; full = all dates",
    )
    FULL_REFRESH: bool = Field(default=False, description="Alias for SYNC_MODE=full when true")
    START_DATE: str | None = Field(
        default=None,
        description="First date (YYYY-MM-DD) for initial incremental run when no state exists",
    )

    # API-only
    API_CACHE_MODE: Literal["remote", "materialize"] = Field(
        default="remote",
        description=(
            "remote = query Parquet directly each request; "
            "materialize = load rollup into local DuckDB table at startup for faster requests"
        ),
    )

    @model_validator(mode="after")
    def require_storage(self) -> "Settings":
        if not self.LOCAL_STORAGE_PATH and not self.GCS_BUCKET:
            raise ValueError("Set either LOCAL_STORAGE_PATH (local testing) or GCS_BUCKET (GCP)")
        return self

    @property
    def s3_base_uri(self) -> str:
        return f"s3://{self.S3_BUCKET}/{self.S3_PREFIX}"

    @property
    def storage_base_uri(self) -> str:
        """URI for Parquet/state: file:// when local, gs:// when GCS."""
        if self.LOCAL_STORAGE_PATH:
            path = os.path.abspath(os.path.join(self.LOCAL_STORAGE_PATH, self.GCS_PREFIX))
            return path.replace("\\", "/")  # DuckDB and file:// like forward slashes
        return f"gs://{self.GCS_BUCKET}/{self.GCS_PREFIX}"

    @property
    def gcs_base_uri(self) -> str:
        return f"gs://{self.GCS_BUCKET}/{self.GCS_PREFIX}"

    @property
    def is_local_storage(self) -> bool:
        return bool(self.LOCAL_STORAGE_PATH)

    @property
    def is_full_refresh(self) -> bool:
        return self.FULL_REFRESH or self.SYNC_MODE == "full"


@lru_cache
def get_settings() -> Settings:
    return Settings()
