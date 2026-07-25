# Databricks notebook source

"""
Mock Bronze ingestion layer for testing the pipeline remediation agent.

This notebook simulates a DLT Bronze table by creating a small in-memory
dataset of raw events. It is intentionally simple so that the remediation
agent test harness can focus on Silver-layer expectation failures rather
than data availability issues.
"""

import dlt
from pyspark.sql import SparkSession


@dlt.table(
    name="bronze_raw_events",
    comment="Mock raw events ingested at the Bronze layer for remediation-agent testing.",
)
def bronze_raw_events():
    spark = SparkSession.builder.getOrCreate()

    mock_events = [
        {"id": 1, "event_type": "click", "user_id": "u001", "timestamp": "2024-01-15T10:00:00Z"},
        {"id": 2, "event_type": "view",  "user_id": "u002", "timestamp": "2024-01-15T10:01:00Z"},
        {"id": 3, "event_type": "click", "user_id": "u003", "timestamp": "2024-01-15T10:02:00Z"},
        {"id": None, "event_type": "purchase", "user_id": "u004", "timestamp": "2024-01-15T10:03:00Z"},
        {"id": 5, "event_type": "view",  "user_id": "u005", "timestamp": "2024-01-15T10:04:00Z"},
        {"id": 6, "event_type": "logout", "user_id": "u001", "timestamp": "2024-01-15T10:05:00Z"},
    ]

    return spark.createDataFrame(mock_events)
