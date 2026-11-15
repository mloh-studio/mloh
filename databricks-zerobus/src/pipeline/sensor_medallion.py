from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.active()
SOURCE_TABLE = spark.conf.get("zerobus_demo.source_table")
MODELLED_EVENT_TYPES = ["reading", "alert"]
DEDUP_WINDOW = "10 minutes"

READING_RULES = {
    "valid_temperature": "temperature_c BETWEEN -50 AND 80",
    "valid_humidity": "humidity_pct BETWEEN 0 AND 100",
    "has_device": "device_id IS NOT NULL",
}
BREAKS_ANY_READING_RULE = "NOT ({})".format(" AND ".join(f"({rule})" for rule in READING_RULES.values()))


def payload_field(path: str, sql_type: str):
    return F.expr(f"try_variant_get(payload, '$.{path}', '{sql_type}')")


@dp.temporary_view
def unique_events():
    return (
        spark.readStream.table(SOURCE_TABLE)
        .withWatermark("event_ts", DEDUP_WINDOW)
        .dropDuplicatesWithinWatermark(["event_id"])
    )


@dp.temporary_view
def readings():
    seconds_since_event = F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp("event_ts")
    return (
        spark.readStream.table("unique_events")
        .where("event_type = 'reading'")
        .select(
            "event_id", "device_id", "event_ts", "schema_version", "ingest_source",
            payload_field("site", "string").alias("site"),
            payload_field("temperature_c", "double").alias("temperature_c"),
            payload_field("humidity_pct", "double").alias("humidity_pct"),
            payload_field("battery_pct", "int").alias("battery_pct"),
            payload_field("firmware.version", "string").alias("firmware_version"),
            payload_field("tags", "array<string>").alias("tags"),
            seconds_since_event.alias("ingest_latency_s"),
        )
    )


@dp.table(comment="Valid sensor readings with typed columns. One row per event_id.")
@dp.expect_all_or_drop(READING_RULES)
def sensor_readings_silver():
    return spark.readStream.table("readings")


@dp.table(comment="Readings that failed validation.")
def sensor_readings_quarantine():
    return spark.readStream.table("readings").where(BREAKS_ANY_READING_RULE)


@dp.table(comment="Device alerts with typed columns. One row per event_id.")
@dp.expect_or_drop("has_code", "code IS NOT NULL")
def sensor_alerts_silver():
    return (
        spark.readStream.table("unique_events")
        .where("event_type = 'alert'")
        .select(
            "event_id", "device_id", "event_ts", "schema_version",
            payload_field("site", "string").alias("site"),
            payload_field("code", "string").alias("code"),
            payload_field("severity", "string").alias("severity"),
            payload_field("message", "string").alias("message"),
            payload_field("context.threshold", "double").alias("threshold"),
        )
    )


@dp.table(comment="Events whose event_type has no silver table yet.")
def unknown_events():
    return (
        spark.readStream.table("unique_events")
        .where(~F.col("event_type").isin(MODELLED_EVENT_TYPES))
    )


@dp.materialized_view(comment="Per-device readings summarised by minute.")
def device_metrics_1m():
    minute = F.window("event_ts", "1 minute")
    return (
        spark.read.table("sensor_readings_silver")
        .groupBy(minute.alias("minute"), "site", "device_id")
        .agg(
            F.count("*").alias("readings"),
            F.round(F.avg("temperature_c"), 2).alias("avg_temp_c"),
            F.round(F.max("temperature_c"), 2).alias("max_temp_c"),
            F.round(F.avg("humidity_pct"), 2).alias("avg_humidity_pct"),
            F.min("battery_pct").alias("min_battery_pct"),
            F.max("firmware_version").alias("latest_firmware"),
        )
        .select(
            F.col("minute.start").alias("window_start"),
            F.col("minute.end").alias("window_end"),
            "site", "device_id", "readings", "avg_temp_c", "max_temp_c",
            "avg_humidity_pct", "min_battery_pct", "latest_firmware",
        )
    )
