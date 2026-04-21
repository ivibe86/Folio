"""
sync_status.py
In-memory sync job tracking for frontend status polling.
"""

from __future__ import annotations

from datetime import datetime
from threading import Lock
from typing import Any
import uuid


_lock = Lock()
_active_jobs: dict[str, dict[str, Any]] = {}
_last_snapshot: dict[str, Any] = {
    "active": False,
    "job_id": None,
    "source": None,
    "phase": None,
    "detail": None,
    "started_at": None,
    "completed_at": None,
    "status": "idle",
    "error": None,
}


def _now() -> str:
    return datetime.now().isoformat()


def start_sync(source: str, phase: str = "starting", detail: str | None = None) -> str:
    job_id = uuid.uuid4().hex
    started_at = _now()
    job = {
        "job_id": job_id,
        "source": source,
        "phase": phase,
        "detail": detail,
        "started_at": started_at,
        "completed_at": None,
        "status": "running",
        "error": None,
    }
    with _lock:
        _active_jobs[job_id] = job
        _last_snapshot.update(
            {
                "active": True,
                **job,
            }
        )
    return job_id


def update_phase(job_id: str, phase: str, detail: str | None = None):
    with _lock:
        job = _active_jobs.get(job_id)
        if not job:
            return
        job["phase"] = phase
        job["detail"] = detail
        _last_snapshot.update(
            {
                "active": True,
                **job,
            }
        )


def finish_sync(job_id: str, status: str = "completed", error: str | None = None):
    completed_at = _now()
    with _lock:
        job = _active_jobs.pop(job_id, None)
        if job is None:
            return

        final_snapshot = {
            **job,
            "completed_at": completed_at,
            "status": status,
            "error": error,
            "active": bool(_active_jobs),
        }
        _last_snapshot.update(final_snapshot)

        if _active_jobs:
            # Keep the newest remaining active job as the current live status.
            newest = max(_active_jobs.values(), key=lambda item: item["started_at"])
            _last_snapshot.update(
                {
                    "active": True,
                    **newest,
                }
            )


def get_sync_status() -> dict[str, Any]:
    with _lock:
        return dict(_last_snapshot)
