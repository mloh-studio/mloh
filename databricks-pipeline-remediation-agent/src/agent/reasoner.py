"""Agentic reasoning layer for pipeline failure events.

Calls the Databricks Foundation Model API via MLflow to produce a structured
explanation of the error and actionable remediation suggestions.

The model endpoint is configurable via the DATABRICKS_LLM_ENDPOINT env var
(default: databricks-meta-llama-3-3-70b-instruct).
"""
import json
import logging
import os
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

_DEFAULT_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"

_SYSTEM_PROMPT = """
You are an expert Databricks data engineering assistant. You help data engineers
understand why a Delta Live Tables (DLT) pipeline has failed and how to fix it.

When given a pipeline error, you respond ONLY with a valid JSON object in this exact shape:
{
  "summary": "<1-2 sentence plain-English explanation of what went wrong>",
  "root_cause": "<the single most likely technical root cause>",
  "suggestions": [
    "<actionable fix step 1>",
    "<actionable fix step 2>",
    "<actionable fix step 3>"
  ]
}

Be specific and concrete. Reference the pipeline name and error type in your summary.
Do not include any text outside the JSON object.
"""


@dataclass
class ReasoningResult:
    summary: str
    root_cause: str
    suggestions: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "summary": self.summary,
            "root_cause": self.root_cause,
            "suggestions": self.suggestions,
        }


def reason(event: dict[str, Any], client: Any = None) -> ReasoningResult:
    """Call the LLM to explain the error and suggest remediation steps.

    Args:
        event:  Enriched event dict (pipeline_name, error_type, error_message, etc.)
        client: An MLflow deployments client. If None, one is created automatically
                using the DATABRICKS_HOST / DATABRICKS_TOKEN environment variables.

    Returns:
        ReasoningResult with summary, root_cause, and suggestions.
        Falls back to a safe default if the LLM call fails.
    """
    if client is None:
        client = _get_default_client()

    endpoint = os.getenv("DATABRICKS_LLM_ENDPOINT", _DEFAULT_ENDPOINT)
    user_message = _build_user_message(event)

    try:
        response = client.predict(
            endpoint=endpoint,
            inputs={
                "messages": [
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {"role": "user", "content": user_message},
                ],
                "max_tokens": 512,
                "temperature": 0.2,
            },
        )
        content = response["choices"][0]["message"]["content"]
        return _parse_response(content)
    except Exception as exc:
        logger.error("LLM reasoning failed: %s — falling back to raw error details", exc)
        return _fallback_result(event)


def _get_default_client() -> Any:
    try:
        import mlflow.deployments
        return mlflow.deployments.get_deploy_client("databricks")
    except Exception as exc:
        raise RuntimeError(
            "Could not create MLflow deployments client. "
            "Ensure DATABRICKS_HOST and DATABRICKS_TOKEN are set."
        ) from exc


def _build_user_message(event: dict[str, Any]) -> str:
    lines = [
        f"Pipeline name: {event['pipeline_name']}",
        f"Error type: {event['error_type']}",
        f"Error message: {event['error_message']}",
        "",
        "Please explain this failure and suggest how to fix it.",
    ]
    return "\n".join(lines)

def _parse_response(content: str) -> ReasoningResult:
    """Parse the LLM JSON response into a ReasoningResult."""
    try:
        # Strip markdown code fences if the model wrapped the JSON
        cleaned = content.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("```")[1]
            if cleaned.startswith("json"):
                cleaned = cleaned[4:]
        data = json.loads(cleaned.strip())
        return ReasoningResult(
            summary=data["summary"],
            root_cause=data["root_cause"],
            suggestions=data["suggestions"],
        )
    except (json.JSONDecodeError, KeyError) as exc:
        logger.warning("Could not parse LLM response as JSON: %s", exc)
        return ReasoningResult(
            summary=content[:300],
            root_cause="Unable to parse structured response from LLM.",
            suggestions=["Review the raw error message and pipeline logs."],
        )


def _fallback_result(event: dict[str, Any]) -> ReasoningResult:
    """Return a safe default when the LLM call fails entirely."""
    return ReasoningResult(
        summary=(
            f"Pipeline '{event['pipeline_name']}' failed with error type "
            f"'{event['error_type']}': {event['error_message']}"
        ),
        root_cause="LLM reasoning unavailable — see raw error message for details.",
        suggestions=[
            "Check the pipeline event log in the Databricks UI for full stack traces.",
            "Review recent schema or code changes to the affected tables.",
            "Retry the pipeline update after addressing the reported error.",
        ],
    )
