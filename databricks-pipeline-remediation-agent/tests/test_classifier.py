"""Unit tests for the error classifier module.

Tests verify behaviour through the public interface: classify(error_type) -> Classification.
Expected values come from the spec, not derived from the implementation.
"""
import pytest
from src.agent.classifier import Classification, classify


class TestTransientErrors:
    def test_rate_limit_is_transient(self):
        assert classify("RATE_LIMIT") == Classification.TRANSIENT

    def test_request_timeout_is_transient(self):
        assert classify("REQUEST_TIMEOUT") == Classification.TRANSIENT

    def test_temporarily_unavailable_is_transient(self):
        assert classify("TEMPORARILY_UNAVAILABLE") == Classification.TRANSIENT


class TestNonTransientErrors:
    def test_schema_mismatch_is_non_transient(self):
        assert classify("SCHEMA_MISMATCH") == Classification.NON_TRANSIENT

    def test_expectation_failure_is_non_transient(self):
        assert classify("EXPECTATION_FAILURE") == Classification.NON_TRANSIENT

    def test_configuration_error_is_non_transient(self):
        assert classify("CONFIGURATION_ERROR") == Classification.NON_TRANSIENT

    def test_resource_does_not_exist_is_non_transient(self):
        assert classify("RESOURCE_DOES_NOT_EXIST") == Classification.NON_TRANSIENT

    def test_parse_exception_is_non_transient(self):
        assert classify("PARSE_EXCEPTION") == Classification.NON_TRANSIENT

    def test_internal_error_is_non_transient(self):
        assert classify("INTERNAL_ERROR") == Classification.NON_TRANSIENT


class TestUnknownErrors:
    def test_unknown_error_defaults_to_non_transient(self):
        """Fail-safe: novel errors must never be silently dropped."""
        assert classify("SOME_FUTURE_ERROR_TYPE") == Classification.NON_TRANSIENT

    def test_empty_string_defaults_to_non_transient(self):
        assert classify("") == Classification.NON_TRANSIENT

    def test_none_defaults_to_non_transient(self):
        assert classify(None) == Classification.NON_TRANSIENT
