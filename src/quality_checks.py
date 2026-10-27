from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

RAW_SCHEMA = "RAW"
RAW_TABLE = "WEATHER_FORECAST"

# How many prior runs to average over, and the minimum number of them needed
# before the check is meaningful at all. A brand new warehouse has no history
# to compare against, so the check reports itself as skipped rather than
# failing the first few runs of the pipeline.
RECENT_RUN_LIMIT = 7
MIN_RUNS_FOR_CHECK = 3

# A run is anomalous when it loads less than this fraction of the rolling
# average. Catches a partial or truncated API pull that still produced rows.
ANOMALY_THRESHOLD_RATIO = 0.5


def evaluate_volume_anomaly(
    current_count: int,
    recent_counts: list[int],
) -> dict:
    """Decide whether current_count is anomalously low against recent history.

    Pure function with no database access so the thresholds can be unit tested
    directly. Returns a result dict rather than raising, leaving the decision
    of what to do about an anomaly to the caller.
    """
    if len(recent_counts) < MIN_RUNS_FOR_CHECK:
        return {
            "checked": False,
            "reason": "insufficient_history",
            "current_count": current_count,
            "runs_available": len(recent_counts),
        }

    rolling_avg = sum(recent_counts) / len(recent_counts)
    threshold = rolling_avg * ANOMALY_THRESHOLD_RATIO

    return {
        "checked": True,
        "anomalous": current_count < threshold,
        "current_count": current_count,
        "rolling_avg": rolling_avg,
        "threshold": threshold,
        "runs_available": len(recent_counts),
    }


def fetch_run_volumes(conn, current_run_id: str) -> tuple[int, list[int]]:
    """Return this run's row count and the row counts of recent prior runs.

    conn is a snowflake.connector connection. Bind parameters use the
    connector's pyformat style, %(name)s, never string formatting.
    """
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"""
            SELECT COUNT(*) FROM {RAW_SCHEMA}.{RAW_TABLE}
            WHERE dag_run_id = %(run_id)s
            """,
            {"run_id": current_run_id},
        )
        current_count = cursor.fetchone()[0]

        cursor.execute(
            f"""
            SELECT dag_run_id, COUNT(*) AS row_count
            FROM {RAW_SCHEMA}.{RAW_TABLE}
            WHERE dag_run_id != %(run_id)s
              AND dag_run_id IS NOT NULL
            GROUP BY dag_run_id
            ORDER BY MIN(ingested_at) DESC
            LIMIT {RECENT_RUN_LIMIT}
            """,
            {"run_id": current_run_id},
        )
        recent_runs = cursor.fetchall()
    finally:
        cursor.close()

    return current_count, [row[1] for row in recent_runs]


def check_volume_anomaly(conn, current_run_id: str) -> dict:
    """Run the volume anomaly check, raising ValueError if the run looks short."""
    current_count, recent_counts = fetch_run_volumes(conn, current_run_id)
    result = evaluate_volume_anomaly(current_count, recent_counts)

    if not result["checked"]:
        logger.info(
            "Not enough run history for anomaly check (%d previous runs found), skipping",
            result["runs_available"],
        )
        return result

    logger.info(
        "Volume check: current=%d, rolling_avg=%.1f, threshold=%.1f",
        result["current_count"], result["rolling_avg"], result["threshold"],
    )

    if result["anomalous"]:
        raise ValueError(
            f"Volume anomaly detected: current run ({current_run_id}) loaded "
            f"{result['current_count']} rows, more than "
            f"{(1 - ANOMALY_THRESHOLD_RATIO):.0%} below rolling average of "
            f"{result['rolling_avg']:.1f}"
        )

    return result
