"""Unit tests for the notifier module.

Tests verify external behaviour: what gets written to the Delta table and
what gets posted to Teams. The Delta write and HTTP call are both mocked.
The ReasoningResult is constructed directly — no LLM call is made in these tests.
"""
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.agent.classifier import Classification
from src.agent.notifier import notify
from src.agent.reasoner import ReasoningResult


SAMPLE_EVENT = {
    "pipeline_id": "pipe-001",
    "pipeline_name": "test_pipeline",
    "update_id": "upd-abc",
    "error_type": "SCHEMA_MISMATCH",
    "error_message": "Column 'id' not found in schema",
    "classification": Classification.NON_TRANSIENT,
}

SAMPLE_REASONING = ReasoningResult(
    summary="The pipeline failed because column 'id' is missing from the schema.",
    root_cause="A recent schema change removed the 'id' column from the source table.",
    suggestions=[
        "Add the 'id' column back to the source table schema.",
        "Update the DLT pipeline expectations to match the new schema.",
        "Validate schema compatibility before deploying pipeline changes.",
    ],
)


class TestMockMode:
    """When TEAMS_WEBHOOK_URL is not set, notifications go to Delta only."""

    def test_writes_correct_row_to_delta(self):
        mock_spark = MagicMock()
        with patch("src.agent.notifier.os.getenv", return_value=None):
            notify(SAMPLE_EVENT, reasoning=SAMPLE_REASONING, spark=mock_spark)

        mock_spark.createDataFrame.assert_called_once()
        rows = mock_spark.createDataFrame.call_args[0][0]
        assert len(rows) == 1
        row = rows[0]
        assert row["pipeline_id"] == "pipe-001"
        assert row["pipeline_name"] == "test_pipeline"
        assert row["update_id"] == "upd-abc"
        assert row["error_type"] == "SCHEMA_MISMATCH"
        assert row["classification"] == "NON_TRANSIENT"
        assert row["notification_sent"] is True

    def test_delta_row_includes_llm_reasoning_fields(self):
        mock_spark = MagicMock()
        with patch("src.agent.notifier.os.getenv", return_value=None):
            notify(SAMPLE_EVENT, reasoning=SAMPLE_REASONING, spark=mock_spark)

        row = mock_spark.createDataFrame.call_args[0][0][0]
        assert row["llm_summary"] == SAMPLE_REASONING.summary
        assert row["llm_root_cause"] == SAMPLE_REASONING.root_cause
        assert "llm_suggestions" in row

    def test_does_not_post_to_teams_when_url_not_set(self):
        mock_spark = MagicMock()
        with (patch("src.agent.notifier.os.getenv", return_value=None),
              patch("src.agent.notifier.requests.post") as mock_post):
            notify(SAMPLE_EVENT, reasoning=SAMPLE_REASONING, spark=mock_spark)
            mock_post.assert_not_called()

    def test_transient_event_is_not_written_to_delta(self):
        mock_spark = MagicMock()
        transient_event = {**SAMPLE_EVENT, "classification": Classification.TRANSIENT}
        with patch("src.agent.notifier.os.getenv", return_value=None):
            notify(transient_event, reasoning=SAMPLE_REASONING, spark=mock_spark)
        mock_spark.createDataFrame.assert_not_called()


class TestRealMode:
    """When TEAMS_WEBHOOK_URL is set, notifications go to Delta AND Teams."""

    def test_posts_to_teams_when_url_is_set(self):
        mock_spark = MagicMock()
        teams_url = "https://teams.example.com/webhook"
        with (patch("src.agent.notifier.os.getenv", return_value=teams_url),
              patch("src.agent.notifier.requests.post") as mock_post):
            mock_post.return_value.status_code = 200
            notify(SAMPLE_EVENT, reasoning=SAMPLE_REASONING, spark=mock_spark)

        mock_post.assert_called_once()
        assert mock_post.call_args[0][0] == teams_url

    def test_teams_payload_contains_pipeline_name_and_reasoning(self):
        mock_spark = MagicMock()
        teams_url = "https://teams.example.com/webhook"
        with (patch("src.agent.notifier.os.getenv", return_value=teams_url),
              patch("src.agent.notifier.requests.post") as mock_post):
            mock_post.return_value.status_code = 200
            notify(SAMPLE_EVENT, reasoning=SAMPLE_REASONING, spark=mock_spark)

        payload_str = str(mock_post.call_args[1]["json"])
        assert "test_pipeline" in payload_str
        assert "SCHEMA_MISMATCH" in payload_str
        assert SAMPLE_REASONING.root_cause in payload_str
        assert SAMPLE_REASONING.suggestions[0] in payload_str
