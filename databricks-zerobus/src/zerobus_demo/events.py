"""Fake IoT sensor events: a few typed metadata columns plus a free-form payload."""

from __future__ import annotations

import json
import random
import time
import uuid
from dataclasses import dataclass

SITES = ["london", "manchester", "dublin", "edinburgh"]
ALERT_CODES = ["OVERHEAT", "LOW_BATTERY", "DOOR_OPEN", "SENSOR_DRIFT"]
ALERT_SEVERITIES = ["LOW", "MEDIUM", "HIGH"]
FIRMWARE_VERSIONS = ["2.4.1", "2.5.0", "3.0.0-rc1"]
DEVICE_TAGS = ["indoor", "outdoor", "cold-chain", "pilot"]
IMPOSSIBLE_TEMPERATURES = [-273.5, 999.0]

TABLE_COLUMNS = (
    "event_id", "event_type", "schema_version", "device_id",
    "event_ts", "ingest_source", "payload",
)
VARIANT_COLUMNS = ("payload",)


def to_wire(event: dict) -> dict:
    # Zerobus rejects a nested object for a VARIANT column (error 4044); it wants a JSON string.
    return {
        column: json.dumps(value, separators=(",", ":")) if column in VARIANT_COLUMNS else value
        for column, value in event.items()
    }


def epoch_micros_now() -> int:
    # Zerobus takes TIMESTAMP columns as integer microseconds, not ISO strings.
    return time.time_ns() // 1_000


@dataclass
class SensorEventGenerator:
    num_devices: int = 50
    bad_record_ratio: float = 0.02
    v2_ratio: float = 0.3
    alert_ratio: float = 0.05
    seed: int | None = None

    def __post_init__(self) -> None:
        self._rng = random.Random(self.seed)
        self._devices = [
            (f"sensor-{i:04d}", SITES[i % len(SITES)]) for i in range(self.num_devices)
        ]

    def next_event(self, source: str = "local-producer") -> dict:
        device_id, site = self._rng.choice(self._devices)

        if self._chance(self.alert_ratio):
            event_type, schema_version = "alert", 1
            payload = self._alert(site)
        else:
            event_type = "reading"
            schema_version = 2 if self._chance(self.v2_ratio) else 1
            payload = self._reading(site, schema_version)

        return {
            "event_id": self._uuid(),
            "event_type": event_type,
            "schema_version": schema_version,
            "device_id": device_id,
            "event_ts": epoch_micros_now(),
            "ingest_source": source,
            "payload": payload,
        }

    def batch(self, size: int, source: str = "local-producer") -> list[dict]:
        return [self.next_event(source) for _ in range(size)]

    def wire_batch(self, size: int, source: str = "local-producer") -> list[dict]:
        return [to_wire(event) for event in self.batch(size, source)]

    def _reading(self, site: str, schema_version: int) -> dict:
        reading = {
            "site": site,
            "temperature_c": self._temperature(),
            "humidity_pct": round(self._clamp(self._rng.gauss(45.0, 10.0), 0.0, 100.0), 2),
            "battery_pct": self._rng.randint(5, 100),
        }
        if schema_version >= 2:
            reading["firmware"] = {
                "version": self._rng.choice(FIRMWARE_VERSIONS),
                "uptime_s": self._rng.randint(60, 2_000_000),
            }
            reading["tags"] = self._rng.sample(DEVICE_TAGS, k=self._rng.randint(0, 2))
        return reading

    def _alert(self, site: str) -> dict:
        return {
            "site": site,
            "code": self._rng.choice(ALERT_CODES),
            "severity": self._rng.choice(ALERT_SEVERITIES),
            "message": "threshold breached",
            "context": {"threshold": self._rng.choice([30, 35, 40]), "window_s": 60},
        }

    def _temperature(self) -> float:
        if self._chance(self.bad_record_ratio):
            return self._rng.choice(IMPOSSIBLE_TEMPERATURES)
        return round(self._rng.gauss(21.0, 3.5), 2)

    def _chance(self, probability: float) -> bool:
        return self._rng.random() < probability

    def _uuid(self) -> str:
        return str(uuid.UUID(int=self._rng.getrandbits(128), version=4))

    @staticmethod
    def _clamp(value: float, low: float, high: float) -> float:
        return min(max(value, low), high)
