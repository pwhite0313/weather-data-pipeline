"""One line to a phone when a task fails.

ntfy.sh is a free push service with no account: anything POSTed to
https://ntfy.sh/<topic> reaches every device subscribed to that topic. The
topic name is the only secret, so it is random and lives in .env, never here.
"""
from __future__ import annotations

import logging
import os

import requests

logger = logging.getLogger(__name__)

NTFY_URL = "https://ntfy.sh"
TOPIC_ENV = "NTFY_TOPIC"
TIMEOUT_SECONDS = 10


def failure_message(dag_id: str, task_id: str, try_number: int, log_url: str | None) -> str:
    line = f"{dag_id}.{task_id} try {try_number} failed"
    return f"{line}\n{log_url}" if log_url else line


def notify_failure(dag_id: str, task_id: str, try_number: int, log_url: str | None, post=requests.post) -> bool:
    """POST one line to the ntfy topic. Returns False, and logs why, when the
    topic is not configured or the post fails; a broken alert must never turn
    a task failure into a second exception."""
    topic = os.getenv(TOPIC_ENV)
    if not topic:
        logger.warning("%s is not set; failure not sent to a phone", TOPIC_ENV)
        return False

    body = failure_message(dag_id, task_id, try_number, log_url)
    try:
        response = post(
            f"{NTFY_URL}/{topic}",
            data=body.encode("utf-8"),
            headers={"Title": f"{dag_id} failed", "Priority": "high", "Tags": "rotating_light"},
            timeout=TIMEOUT_SECONDS,
        )
        response.raise_for_status()
    except Exception:
        logger.exception("ntfy post failed; the task failure stands on its own")
        return False

    logger.info("Failure sent to ntfy topic")
    return True
