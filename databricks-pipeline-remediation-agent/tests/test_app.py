"""Integration tests for the FastAPI webhook endpoint.

Tests use the FastAPI TestClient — no live Databricks cluster required.
Both the reasoner LLM call and the notifier's Delta/Teams calls are mocked
so tests are self-contained and fast.
"""
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from src.agent.app import app
from src.agent.reasoner import ReasoningResult


MOCK_REASONING = ReasoningResult(
    summary="The pipeline failed due to a schema mismatch.",
    root_cause="Column 'id' was removed from the upstream source.",
    suggestions=[
        "Restore the 'id' column in the source table.",
        "Update pipeline schema expectations.",
        "Re-run the pipeline after the fix.",
    ],
)


@pytest.fixture
def client():
    return TestClient(app)


NON_TRANSIENT_PAYLOAD = {
    "pipeline_id": "pipe-001",
    "pipeline_name": "test_pipeline",
    "update_id": "upd-abc",
    "event_type": "PIPELINE_UPDATE_FAILED",
    "error": {
        "error_type": "SCHEMA_MISMATCH",
        "message": "Column 'id' not found in schema",
    },
}

TRANSIENT_PAYLOAD = {
    "pipeline_id": "pipe-002",
    "pipeline_name": "test_pipeline",
    "update_id": "upd-xyz",
    "event_type": "PIPELINE_UPDATE_FAILED",
    "error": {
        "error_type": "RATE_LIMIT",
        "message": "Too many requests",
    },
}


class TestWebhookEndpoint:
    def test_returns_200_for_valid_payload(self, client):
        with (patch("src.agent.app.reason", return_value=MOCK_REASONING),
              patch("src.agent.app.notify"),
              patch("src.agent.app.get_spark")):
            response = client.post("/webhook", json=NON_TRANSIENT_PAYLOAD)
        assert response.status_code == 200

    def test_non_transient_error_triggers_notification(self, client):
        with (patch("src.agent.app.reason", return_value=MOCK_REASONING),
              patch("src.agent.app.notify") as mock_notify,
              patch("src.agent.app.get_spark")):
            client.post("/webhook", json=NON_TRANSIENT_PAYLOAD)

        mock_notify.assert_called_once()
        event_arg = mock_notify.call_args[0][0]
        assert event_arg["classification"].value == "NON_TRANSIENT"
        assert event_arg["error_type"] == "SCHEMA_MISMATCH"

    def test_non_transient_error_calls_reasoner(self, client):
        with (patch("src.agent.app.reason", return_value=MOCK_REASONING) as mock_reason,
              patch("src.agent.app.notify"),
              patch("src.agent.app.get_spark")):
            client.post("/webhook", json=NON_TRANSIENT_PAYLOAD)

        mock_reason.assert_called_once()

    def test_transient_error_suppresses_notification_and_reasoning(self, client):
        with (patch("src.agent.app.reason") as mock_reason,
              patch("src.agent.app.notify") as mock_notify,
              patch("src.agent.app.get_spark")):
            response = client.post("/webhook", json=TRANSIENT_PAYLOAD)

        mock_reason.assert_not_called()
        mock_notify.assert_not_called()
        body = response.json()
        assert body["classification"] == "TRANSIENT"
        assert body["notification_sent"] is False
        assert body["reasoning"] is None

    def test_response_body_contains_classification_and_reasoning(self, client):
        with (patch("src.agent.app.reason", return_value=MOCK_REASONING),
              patch("src.agent.app.notify"),
              patch("src.agent.app.get_spark")):
            response = client.post("/webhook", json=NON_TRANSIENT_PAYLOAD)

        body = response.json()
        assert body["classification"] == "NON_TRANSIENT"
        assert body["notification_sent"] is True
        assert body["reasoning"]["summary"] == MOCK_REASONING.summary
        assert body["reasoning"]["root_cause"] == MOCK_REASONING.root_cause
        assert len(body["reasoning"]["suggestions"]) == 3

    def test_returns_422_for_missing_error_field(self, client):
        bad_payload = {k: v for k, v in NON_TRANSIENT_PAYLOAD.items() if k != "error"}
        response = client.post("/webhook", json=bad_payload)
        assert response.status_code == 422

    def test_health_endpoint_returns_ok(self, client):
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}
