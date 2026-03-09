"""Shared API dependencies (DB access + optional TTL cache refresh)."""

from __future__ import annotations

from datetime import datetime, timezone
import logging
from threading import Lock

from duckdb import DuckDBPyConnection
from fastapi import Request

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def get_db(request: Request) -> DuckDBPyConnection:
    """Return the app DuckDB connection, refreshing materialized cache when stale."""
    app_state = request.app.state
    con: DuckDBPyConnection = app_state.db

    if getattr(app_state, "api_cache_mode", "remote") != "materialize":
        return con

    refresh_every = int(getattr(app_state, "api_refresh_interval_seconds", 24 * 60 * 60))
    last_refresh: datetime | None = getattr(app_state, "last_materialized_at", None)
    now = _utcnow()
    is_stale = last_refresh is None or (now - last_refresh).total_seconds() >= refresh_every
    if not is_stale:
        return con

    refresh_lock: Lock | None = getattr(app_state, "cache_refresh_lock", None)
    if refresh_lock is None:
        return con

    with refresh_lock:
        # Double-check inside the lock to avoid duplicate refresh on races.
        last_refresh = getattr(app_state, "last_materialized_at", None)
        now = _utcnow()
        is_stale = last_refresh is None or (now - last_refresh).total_seconds() >= refresh_every
        if not is_stale:
            return con
        refresh_fn = getattr(app_state, "refresh_rollup_cache", None)
        if refresh_fn is None:
            return con
        refreshed_at = refresh_fn()
        logger.info("Refreshed rollup cache via TTL guard at %s", refreshed_at.isoformat())
    return con
