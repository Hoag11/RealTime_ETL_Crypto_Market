"""Notification helpers that integrate Airflow tasks with chat tools."""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict

import requests


def build_failure_message(context: Dict[str, Any]) -> str:
    """Construct a human readable failure message for Airflow callbacks."""

    task_instance = context.get("task_instance")
    dag_id = getattr(context.get("dag"), "dag_id", "unknown_dag")
    task_id = getattr(task_instance, "task_id", "unknown_task")
    run_id = getattr(task_instance, "run_id", "manual__unknown")
    try_number = getattr(task_instance, "try_number", 0)

    return (
        f"Airflow task failure detected\n"
        f"DAG: {dag_id}\n"
        f"Task: {task_id}\n"
        f"Run ID: {run_id}\n"
        f"Attempt: {try_number}"
    )


def notify_failure(context: Dict[str, Any]) -> None:
    """Send a Slack webhook notification when a task fails."""

    webhook_url = os.getenv("SLACK_WEBHOOK_URL")

    if not webhook_url:
        logging.warning(
            "SLACK_WEBHOOK_URL is not configured; skipping Slack failure notification."
        )
        return

    message = build_failure_message(context)

    try:
        response = requests.post(
            webhook_url,
            data=json.dumps({"text": message}),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        response.raise_for_status()
        logging.info("Sent Slack alert for Airflow task failure: %s", message)
    except requests.RequestException as exc:
        logging.exception("Failed to send Slack notification: %s", exc)


__all__ = ["notify_failure", "build_failure_message"]
