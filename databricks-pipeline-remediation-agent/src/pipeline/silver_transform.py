# Databricks notebook source

"""
Mock Silver transform layer for testing the pipeline remediation agent.

This notebook reads from the Bronze `bronze_raw_events` table and applies
two transformations:

1. `silver_events` — a basic projection/rename pass with no quality checks.
2. `silver_events_validated` — applies a DLT expectation that INTENTIONALLY
   fails on rows where `id` is NULL. The Bronze mock data includes one such
   row to exercise the EXPECTATION_FAILURE remediation path in the agent.
"""

import dlt
import pyspark.sql.functions as F


@dlt.table(
    name="silver_events",
    comment="Silver-layer events: basic cleansing of bronze_raw_events.",
)
def silver_events():
    return (
        dlt.read("bronze_raw_events")
        .withColumn("event_type", F.upper(F.col("event_type")))
        .withColumn("ingested_at", F.current_timestamp())
    )


# NOTE: The row with id=None in the Bronze data is intentional.
# It is inserted specifically to trigger the @dlt.expect_or_fail expectation
# below, causing a DLT EXPECTATION_FAILURE event that the remediation agent
# is designed to detect and classify.
@dlt.table(
    name="silver_events_validated",
    comment="Silver-layer events with strict null-id validation (expect_or_fail).",
)
@dlt.expect_or_fail("valid_id", "id IS NOT NULL")
def silver_events_validated():
    return dlt.read("silver_events")
