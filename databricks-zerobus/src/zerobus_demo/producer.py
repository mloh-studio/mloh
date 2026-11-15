"""Stream sensor events into a Unity Catalog table with the Zerobus gRPC SDK.

    uv run zerobus-produce --records 5000 --rate 1000
"""

from __future__ import annotations

import argparse
import sys
import time

from zerobus.sdk.shared import (
    NonRetriableException,
    RecordType,
    StreamConfigurationOptions,
    TableProperties,
)
from zerobus.sdk.sync import ZerobusSdk

from zerobus_demo.config import add_connection_args, check_connection_args
from zerobus_demo.events import SensorEventGenerator


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    add_connection_args(parser)
    parser.add_argument("--records", type=int, default=5_000)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--rate", type=float, default=0, help="Records per second, 0 for no limit")
    parser.add_argument("--devices", type=int, default=50)
    parser.add_argument("--bad-ratio", type=float, default=0.02)
    parser.add_argument("--source", default="local-producer")
    return parser.parse_args(argv)


def open_stream(args: argparse.Namespace):
    sdk = ZerobusSdk(args.endpoint, args.workspace_url)
    return sdk.create_stream(
        args.client_id,
        args.client_secret,
        TableProperties(args.table),
        StreamConfigurationOptions(record_type=RecordType.JSON),
    )


class Pacer:
    def __init__(self, records_per_second: float) -> None:
        self.rate = records_per_second
        self.started = time.monotonic()

    def wait_until_due(self, records_sent: int) -> None:
        if self.rate <= 0:
            return
        ahead_by = records_sent / self.rate - self.elapsed()
        if ahead_by > 0:
            time.sleep(ahead_by)

    def elapsed(self) -> float:
        return max(time.monotonic() - self.started, 1e-6)


def produce(args: argparse.Namespace) -> int:
    generator = SensorEventGenerator(num_devices=args.devices, bad_record_ratio=args.bad_ratio)
    stream = open_stream(args)
    print(f"Streaming to {args.table}")

    pacer = Pacer(args.rate)
    sent = 0
    try:
        while sent < args.records:
            size = min(args.batch_size, args.records - sent)
            stream.ingest_records_offset(generator.wire_batch(size, source=args.source))
            sent += size
            pacer.wait_until_due(sent)
            if sent % (args.batch_size * 10) == 0 or sent == args.records:
                print(f"  queued {sent:>8,} / {args.records:,}  ({sent / pacer.elapsed():,.0f} rec/s)")

        stream.flush()
    except NonRetriableException as error:
        print(f"Zerobus rejected the write. Check grants and table schema.\n{error}", file=sys.stderr)
        raise
    finally:
        stream.close()

    print(f"Done: {sent:,} records acknowledged in {pacer.elapsed():.1f}s")
    return sent


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    check_connection_args(args)
    produce(args)


if __name__ == "__main__":
    main()
