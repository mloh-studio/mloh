import sys

from pyspark.sql import SparkSession

catalog, schema, landing_table = sys.argv[1:4]
spark = SparkSession.builder.getOrCreate()

TABLES = [
    landing_table,
    "sensor_readings_silver",
    "sensor_readings_quarantine",
    "sensor_alerts_silver",
    "unknown_events",
    "device_metrics_1m",
]


def qualified(table: str) -> str:
    return f"`{catalog}`.`{schema}`.`{table}`"


row_counts = {table: spark.table(qualified(table)).count() for table in TABLES}
width = max(map(len, row_counts))
for table, rows in row_counts.items():
    print(f"{table:<{width}}  {rows:>10,}")

print("\nLanding table by producer and event shape:")
spark.sql(f"""
    SELECT ingest_source, event_type, schema_version, count(*) AS rows, max(event_ts) AS latest_event
    FROM {qualified(landing_table)}
    GROUP BY ALL
    ORDER BY rows DESC
""").show(truncate=False)

print("Latest payload for each event shape:")
spark.sql(f"""
    SELECT event_type, schema_version, payload
    FROM {qualified(landing_table)}
    QUALIFY row_number() OVER (PARTITION BY event_type, schema_version ORDER BY event_ts DESC) = 1
""").show(truncate=False)

if row_counts[landing_table] == 0:
    raise SystemExit("Nothing reached the landing table. The producer didn't deliver.")
if row_counts["sensor_readings_silver"] == 0:
    raise SystemExit("Silver is empty. The pipeline didn't pick up the new rows.")
print("All good.")
