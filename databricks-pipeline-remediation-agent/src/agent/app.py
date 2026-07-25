"""FastAPI webhook receiver for DLT pipeline failure events.

Endpoint: POST /webhook
  - Parses the pipeline event payload
  - Classifies the error type
  - Notifies (non-transient only) via notifier.notify()
  - Returns a structured JSON response

Endpoint: GET /health
  - Liveness check for the Databricks App runtime
"""
import logging
from typing import Any, Optional

from fastapi import FastAPI
from pydantic import BaseModel

from src.agent.classifier import Classification, classify
from src.agent.notifier import notify
from src.agent.reasoner import ReasoningResult, reason

logger = logging.getLogger(__name__)

app = FastAPI(title="Databricks Pipeline Remediation Agent")


# ---------------------------------------------------------------------------
# Pydantic models — the webhook payload contract from the spec
# ---------------------------------------------------------------------------


class PipelineError(BaseModel):
    error_type: str
    message: str


class PipelineEvent(BaseModel):
    pipeline_id: str
    pipeline_name: str
    update_id: str
    event_type: str
    error: PipelineError


class WebhookResponse(BaseModel):
    pipeline_id: str
    update_id: str
    classification: str
    notification_sent: bool
    reasoning: dict | None = None


# ---------------------------------------------------------------------------
# Spark session factory — swapped out for a mock in tests
# ---------------------------------------------------------------------------


def get_spark() -> Optional[Any]:
    """Return a live SparkSession when running on Databricks; None otherwise."""
    try:
        from pyspark.sql import SparkSession  # type: ignore[import-untyped]
        return SparkSession.builder.getOrCreate()
    except ImportError:
        logger.warning("PySpark not available — Delta writes disabled")
        return None


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/webhook", response_model=WebhookResponse)
def webhook(event: PipelineEvent) -> WebhookResponse:
    """Receive a DLT pipeline failure webhook, classify, and notify."""
    classification = classify(event.error.error_type)

    enriched = {
        "pipeline_id": event.pipeline_id,
        "pipeline_name": event.pipeline_name,
        "update_id": event.update_id,
        "error_type": event.error.error_type,
        "error_message": event.error.message,
        "classification": classification,
    }

    notification_sent = False
    reasoning_result: ReasoningResult | None = None

    if classification == Classification.NON_TRANSIENT:
        reasoning_result = reason(enriched)
        spark = get_spark()
        notify(enriched, reasoning=reasoning_result, spark=spark)
        notification_sent = True

    logger.info(
        "Processed webhook for pipeline=%s update=%s classification=%s",
        event.pipeline_id,
        event.update_id,
        classification.value,
    )

    return WebhookResponse(
        pipeline_id=event.pipeline_id,
        update_id=event.update_id,
        classification=classification.value,
        notification_sent=notification_sent,
        reasoning=reasoning_result.to_dict() if reasoning_result else None,
    )
