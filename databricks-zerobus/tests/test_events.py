import json
import time

from zerobus_demo.events import TABLE_COLUMNS, SensorEventGenerator, to_wire


def test_event_matches_table_columns():
    for event in SensorEventGenerator(seed=1).batch(200):
        assert tuple(event) == TABLE_COLUMNS
        assert isinstance(event["payload"], dict)
        json.dumps(event)


def test_timestamp_is_epoch_micros():
    event = SensorEventGenerator(seed=1).next_event()
    assert isinstance(event["event_ts"], int)
    assert abs(event["event_ts"] - time.time() * 1_000_000) < 5_000_000


def test_mixed_event_types_and_versions():
    events = SensorEventGenerator(seed=3, alert_ratio=0.2, v2_ratio=0.5).batch(2_000)
    kinds = {(e["event_type"], e["schema_version"]) for e in events}
    assert kinds == {("reading", 1), ("reading", 2), ("alert", 1)}


def test_v2_readings_carry_new_nested_fields():
    events = SensorEventGenerator(seed=3, alert_ratio=0, v2_ratio=1.0).batch(50)
    assert all("firmware" in e["payload"] and "tags" in e["payload"] for e in events)


def test_bad_records_injected_at_ratio():
    events = SensorEventGenerator(seed=7, bad_record_ratio=0.5, alert_ratio=0).batch(2_000)
    bad = sum(1 for e in events if not -50 <= e["payload"]["temperature_c"] <= 80)
    assert 800 < bad < 1200


def test_unique_ids_and_source_label():
    events = SensorEventGenerator(seed=7).batch(1_000, source="job")
    assert len({e["event_id"] for e in events}) == len(events)
    assert {e["ingest_source"] for e in events} == {"job"}


def test_wire_format_sends_variant_as_json_string():
    gen = SensorEventGenerator(seed=5)
    event = gen.next_event()
    wire = to_wire(event)
    assert isinstance(wire["payload"], str)
    assert json.loads(wire["payload"]) == event["payload"]
    assert {k: v for k, v in wire.items() if k != "payload"} == {
        k: v for k, v in event.items() if k != "payload"}
    assert all(isinstance(e["payload"], str) for e in gen.wire_batch(20))
