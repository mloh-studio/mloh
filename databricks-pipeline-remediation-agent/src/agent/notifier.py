"""Notifier for pipeline remediation events.

In mock mode (TEAMS_WEBHOOK_URL not set):
  - Logs the alert to stdout
  - Writes a row to main.default.pipeline_notifications Delta table

In real mode (TEAMS_WEBHOOK_URL set):
  - Does both of the above AND posts an Adaptive Card to the Teams webhook,
    including the LLM-generated summary, root cause, and fix suggestions.
"""
import logging
import os
from datetime import datetime, timezone
from typing import Any

import requests

from src.agent.classifier import Classification
from src.agent.reasoner import ReasoningResult

logger = logging.getLogger(__name__)

_TABLE = "main.default.pipeline_notifications"


def notify(event: dict[str, Any], reasoning: ReasoningResult, spark: Any) -> None:
    """Raise a notification for a non-transient pipeline error.

    Silently returns for TRANSIENT events. For NON_TRANSIENT events:
      1. Logs to stdout (including LLM summary and suggestions).
      2. Writes to the Delta notifications table (including reasoning fields).
      3. POSTs to Teams if TEAMS_WEBHOOK_URL is set.

    Args:
        event:     Enriched event dict containing classification and error details.
        reasoning: LLM-generated ReasoningResult (summary, root_cause, suggestions).
        spark:     A SparkSession (or mock) used for the Delta write.
    """
    if event["classification"] == Classification.TRANSIENT:
        logger.info(
            "Suppressed transient error %s for pipeline %s",
            event["error_type"],
            event["pipeline_name"],
        )
        return

    _log_alert(event, reasoning)
    _write_to_delta(event, reasoning, spark)

    teams_url = os.getenv("TEAMS_WEBHOOK_URL")
    if teams_url:
        _post_to_teams(event, reasoning, teams_url)


def _log_alert(event: dict[str, Any], reasoning: ReasoningResult) -> None:
    sep = "\n  "
    suggestions_text = sep.join(f"* {s}" for s in reasoning.suggestions)
    msg = (
        f"[ALERT] Non-transient error in pipeline '{event['pipeline_name']}'"
        f" (update: {event['update_id']})"
        f"  Error type:  {event['error_type']}"
        f"  Summary:     {reasoning.summary}"
        f"  Root cause:  {reasoning.root_cause}"
        f"  Suggestions: {suggestions_text}"
    )
    logger.warning(msg)

def _write_to_delta(
    event: dict[str, Any], reasoning: ReasoningResult, spark: Any
) -> None:
    row = {
        "event_time": datetime.now(timezone.utc),
        "pipeline_id": event["pipeline_id"],
        "pipeline_name": event["pipeline_name"],
        "update_id": event["update_id"],
        "error_type": event["error_type"],
        "error_message": event["error_message"],
        "classification": event["classification"].value,
        "llm_summary": reasoning.summary,
        "llm_root_cause": reasoning.root_cause,
        "llm_suggestions": " | ".join(reasoning.suggestions),
        "notification_sent": True,
    }
    df = spark.createDataFrame([row])
    df.write.format("delta").mode("append").saveAsTable(_TABLE)
    logger.info("Notification written to %s", _TABLE)


def _post_to_teams(
    event: dict[str, Any], reasoning: ReasoningResult, url: str
) -> None:
    """POST an Adaptive Card to the Teams incoming webhook."""
    suggestions_facts = [
        {"title": f"Fix {i + 1}", "value": s}
        for i, s in enumerate(reasoning.suggestions)
    ]

    payload = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.4",
                    "body": [
                        {
                            "type": "TextBlock",
                            "text": f"🚨 Pipeline Failure: {event['pipeline_name']}",
                            "weight": "Bolder",
                            "size": "Medium",
                        },
                        {
                            "type": "TextBlock",
                            "text": reasoning.summary,
                            "wrap": True,
                        },
                        {
                            "type": "FactSet",
                            "facts": [
                                {"title": "Pipeline ID", "value": event["pipeline_id"]},
                                {"title": "Update ID", "value": event["update_id"]},
                                {"title": "Error Type", "value": event["error_type"]},
                                {"title": "Root Cause", "value": reasoning.root_cause},
                            ],
                        },
                        {
                            "type": "TextBlock",
                            "text": "**Suggested Fixes**",
                            "weight": "Bolder",
                        },
                        {
                            "type": "FactSet",
                            "facts": suggestions_facts,
                        },
                    ],
                },
            }
        ],
    }
    response = requests.post(url, json=payload)
    logger.info("Teams notification sent, status=%s", response.status_code)
