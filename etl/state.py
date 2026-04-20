"""Read/write ETL state: last_sync cursor and processed-source-file registry."""

from __future__ import annotations

import json
import logging
import os
from datetime import date

from src.config import get_settings

log = logging.getLogger(__name__)


STATE_KEY = "_state/last_sync.json"
PROCESSED_FILES_DIR = "_state/processed_files"


def _state_path() -> str:
    settings = get_settings()
    if settings.is_local_storage:
        return os.path.join(settings.LOCAL_STORAGE_PATH, settings.GCS_PREFIX, STATE_KEY)
    raise RuntimeError("GCS state requires google-cloud-storage")  # handled below


def _processed_files_rel(d: date) -> str:
    return f"{PROCESSED_FILES_DIR}/day={d.isoformat()}.json"


def get_last_sync_date() -> date | None:
    """Load last_sync_date from GCS or local file. Returns None if no state exists."""
    settings = get_settings()
    if settings.is_local_storage:
        path = _state_path()
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            return date.fromisoformat(data["last_sync_date"])
        except FileNotFoundError:
            return None
        except (json.JSONDecodeError, KeyError):
            return None
    from google.cloud import storage
    client = storage.Client()
    blob = client.bucket(settings.GCS_BUCKET).blob(f"{settings.GCS_PREFIX}/{STATE_KEY}")
    try:
        data = json.loads(blob.download_as_string().decode("utf-8"))
        return date.fromisoformat(data["last_sync_date"])
    except Exception as e:
        log.info("No state file or failed to read (will start from beginning): %s", e)
        return None


def set_last_sync_date(d: date) -> None:
    """Write last_sync_date to GCS or local file."""
    settings = get_settings()
    payload = json.dumps({"last_sync_date": d.isoformat()}, indent=2)
    if settings.is_local_storage:
        path = _state_path()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(payload)
        return
    from google.cloud import storage
    client = storage.Client()
    blob = client.bucket(settings.GCS_BUCKET).blob(f"{settings.GCS_PREFIX}/{STATE_KEY}")
    blob.upload_from_string(payload, content_type="application/json")
    log.info("Wrote state to gs://%s/%s/%s", settings.GCS_BUCKET, settings.GCS_PREFIX, STATE_KEY)


def get_processed_keys(d: date) -> set[str]:
    """Load the set of S3 keys already processed for source day `d`."""
    settings = get_settings()
    rel = _processed_files_rel(d)
    if settings.is_local_storage:
        path = os.path.join(settings.LOCAL_STORAGE_PATH, settings.GCS_PREFIX, rel)
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            return set(data.get("keys", []))
        except FileNotFoundError:
            return set()
        except (json.JSONDecodeError, KeyError):
            return set()
    from google.cloud import storage
    client = storage.Client()
    blob = client.bucket(settings.GCS_BUCKET).blob(f"{settings.GCS_PREFIX}/{rel}")
    try:
        data = json.loads(blob.download_as_string().decode("utf-8"))
        return set(data.get("keys", []))
    except Exception:
        return set()


def add_processed_keys(d: date, keys: list[str]) -> None:
    """Union `keys` into the processed-keys manifest for source day `d`."""
    if not keys:
        return
    settings = get_settings()
    rel = _processed_files_rel(d)
    merged = sorted(get_processed_keys(d) | set(keys))
    payload = json.dumps({"day": d.isoformat(), "keys": merged}, indent=2)
    if settings.is_local_storage:
        path = os.path.join(settings.LOCAL_STORAGE_PATH, settings.GCS_PREFIX, rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(payload)
        return
    from google.cloud import storage
    client = storage.Client()
    blob = client.bucket(settings.GCS_BUCKET).blob(f"{settings.GCS_PREFIX}/{rel}")
    blob.upload_from_string(payload, content_type="application/json")
