from __future__ import annotations

import argparse
import os
import sys

REQUIRED = ("endpoint", "workspace_url", "table", "client_id", "client_secret")


def add_connection_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--endpoint", default=os.getenv("ZEROBUS_ENDPOINT"),
                        help="https://<workspace-id>.zerobus.<region>.cloud.databricks.com")
    parser.add_argument("--workspace-url", default=os.getenv("DATABRICKS_HOST"))
    parser.add_argument("--table", default=os.getenv("ZEROBUS_TABLE"),
                        help="catalog.schema.table")
    parser.add_argument("--client-id", default=os.getenv("ZEROBUS_CLIENT_ID"))
    parser.add_argument("--client-secret", default=os.getenv("ZEROBUS_CLIENT_SECRET"))


def check_connection_args(args: argparse.Namespace) -> None:
    missing = [name for name in REQUIRED if not getattr(args, name)]
    if missing:
        sys.exit(f"Missing: {', '.join(missing)}. Pass the flags or set the env vars (see --help).")
    if not args.workspace_url.startswith("http"):
        args.workspace_url = f"https://{args.workspace_url}"
