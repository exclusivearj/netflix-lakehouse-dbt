"""Lightweight Slack notifier for DAG on_failure callbacks.

Reads `slack_webhook` Airflow Connection; if unset, just logs.
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from typing import Any

from airflow.hooks.base import BaseHook


log = logging.getLogger(__name__)


def _webhook_url() -> str | None:
    try:
        conn = BaseHook.get_connection("slack_webhook")
    except Exception:
        return None
    url = conn.password or ""
    return url if url.startswith("http") else None


def post(text: str, blocks: list | None = None) -> None:
    url = _webhook_url()
    if not url:
        log.info("Slack webhook not configured; would have posted: %s", text)
        return
    payload: dict[str, Any] = {"text": text}
    if blocks:
        payload["blocks"] = blocks
    try:
        req = urllib.request.Request(
            url, data=json.dumps(payload).encode(), headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            resp.read()
    except (urllib.error.URLError, urllib.error.HTTPError) as e:
        log.error("Slack post failed: %s", e)


def notify_failure(context: dict) -> None:
    """Airflow on_failure_callback."""
    dag_id = context.get("dag").dag_id if context.get("dag") else "?"
    task_id = context.get("task_instance").task_id if context.get("task_instance") else "?"
    run_id = context.get("run_id", "?")
    post(f":x: DAG `{dag_id}` task `{task_id}` failed (run {run_id}).")
