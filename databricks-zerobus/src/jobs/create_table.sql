CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.' || :schema || '.' || :table) (
  event_id        STRING    NOT NULL COMMENT 'Unique per event. Use it to remove duplicates.',
  event_type      STRING    NOT NULL COMMENT 'What kind of event this is, e.g. reading or alert.',
  schema_version  INT       NOT NULL COMMENT 'Version of the payload shape for this event_type.',
  device_id       STRING             COMMENT 'Sensor that produced the event.',
  event_ts        TIMESTAMP NOT NULL COMMENT 'When the event happened on the device.',
  ingest_source   STRING             COMMENT 'Which producer sent the row.',
  payload         VARIANT            COMMENT 'Event body as sent by the producer.'
)
USING DELTA
CLUSTER BY (event_type, device_id)
COMMENT 'Raw sensor events written by Zerobus Ingest.';
