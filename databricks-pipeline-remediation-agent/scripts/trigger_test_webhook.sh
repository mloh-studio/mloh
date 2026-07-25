#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"

echo "--- Sending NON-TRANSIENT event (SCHEMA_MISMATCH) ---"
curl -s -X POST -H "Content-Type: application/json" -d '{"pipeline_id": "pipeline-001", "pipeline_name": "remediation-agent-mock-pipeline-dev", "error_type": "SCHEMA_MISMATCH", "error_message": "Schema mismatch detected: expected column id of type INT, got STRING", "run_id": "run-abc-001", "timestamp": "2024-01-15T10:10:00Z"}' "${BASE_URL}/webhook" | python3 -m json.tool

echo ""
echo "--- Sending TRANSIENT event (RATE_LIMIT) ---"
curl -s -X POST -H "Content-Type: application/json" -d '{"pipeline_id": "pipeline-001", "pipeline_name": "remediation-agent-mock-pipeline-dev", "error_type": "RATE_LIMIT", "error_message": "Request rate limit exceeded. Retry after 60 seconds.", "run_id": "run-abc-002", "timestamp": "2024-01-15T10:11:00Z"}' "${BASE_URL}/webhook" | python3 -m json.tool

echo ""
echo "Done."
