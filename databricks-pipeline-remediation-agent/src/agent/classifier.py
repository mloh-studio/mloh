"""Error classifier for DLT pipeline webhook events.

Classifies an error_type string as TRANSIENT or NON_TRANSIENT.
Unknown types default to NON_TRANSIENT (fail-safe).
"""
from enum import Enum
from typing import Optional


class Classification(str, Enum):
    TRANSIENT = "TRANSIENT"
    NON_TRANSIENT = "NON_TRANSIENT"


_TRANSIENT_ERRORS: frozenset[str] = frozenset(
    {
        "RATE_LIMIT",
        "REQUEST_TIMEOUT",
        "TEMPORARILY_UNAVAILABLE",
    }
)


def classify(error_type: Optional[str]) -> Classification:
    """Return the classification for a given DLT error type.

    Args:
        error_type: The error_type field from the pipeline webhook payload.
                    None or empty string defaults to NON_TRANSIENT.

    Returns:
        Classification.TRANSIENT if the error is self-healing,
        Classification.NON_TRANSIENT otherwise (including unknowns).
    """
    if error_type and error_type in _TRANSIENT_ERRORS:
        return Classification.TRANSIENT
    return Classification.NON_TRANSIENT
