"""Send sensor events to Zerobus over plain HTTPS, no SDK required.

    uv run zerobus-produce-rest --records 200 --batch-size 50
"""

from __future__ import annotations

import argparse
import base64
import json
import time
import urllib.error
import urllib.parse
import urllib.request

from zerobus_demo.config import add_connection_args, check_connection_args
from zerobus_demo.events import SensorEventGenerator

TOKEN_REFRESH_MARGIN_S = 300
RETRYABLE_STATUS = {429, 500, 502, 503, 504}


def workspace_id_from_endpoint(endpoint: str) -> str:
    url = endpoint if "://" in endpoint else f"https://{endpoint}"
    return urllib.parse.urlparse(url).hostname.split(".")[0]


def authorization_details(table: str) -> list[dict]:
    catalog, schema, _ = table.split(".")
    return [
        {"type": "unity_catalog_privileges", "privileges": ["USE CATALOG"],
         "object_type": "CATALOG", "object_full_path": catalog},
        {"type": "unity_catalog_privileges", "privileges": ["USE SCHEMA"],
         "object_type": "SCHEMA", "object_full_path": f"{catalog}.{schema}"},
        {"type": "unity_catalog_privileges", "privileges": ["SELECT", "MODIFY"],
         "object_type": "TABLE", "object_full_path": table, "operations": ["zerobuswrite"]},
    ]


class TableScopedToken:
    def __init__(self, workspace_url: str, workspace_id: str, table: str,
                 client_id: str, client_secret: str) -> None:
        self._token_url = f"{workspace_url.rstrip('/')}/oidc/v1/token"
        self._basic_auth = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
        self._form = urllib.parse.urlencode({
            "grant_type": "client_credentials",
            "scope": "all-apis",
            "resource": f"api://databricks/workspaces/{workspace_id}/zerobusDirectWriteApi",
            "authorization_details": json.dumps(authorization_details(table)),
        }).encode()
        self._token: str | None = None
        self._expires_at = 0.0

    def get(self) -> str:
        if self._token is None or time.time() > self._expires_at - TOKEN_REFRESH_MARGIN_S:
            self._refresh()
        return self._token

    def _refresh(self) -> None:
        request = urllib.request.Request(self._token_url, data=self._form, method="POST", headers={
            "Authorization": f"Basic {self._basic_auth}",
            "Content-Type": "application/x-www-form-urlencoded",
        })
        with urllib.request.urlopen(request, timeout=30) as response:
            body = json.load(response)
        self._token = body["access_token"]
        self._expires_at = time.time() + int(body.get("expires_in", 3600))


def post_batch(endpoint: str, table: str, token: str, records: list[dict],
               max_attempts: int = 5) -> None:
    url = f"{endpoint.rstrip('/')}/zerobus/v1/tables/{table}/insert"
    body = json.dumps(records).encode()

    for attempt in range(1, max_attempts + 1):
        request = urllib.request.Request(url, data=body, method="POST", headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        })
        try:
            urllib.request.urlopen(request, timeout=30).close()
            return
        except urllib.error.HTTPError as error:
            if error.code not in RETRYABLE_STATUS or attempt == max_attempts:
                detail = error.read().decode(errors="replace")
                raise RuntimeError(f"Zerobus REST {error.code}: {detail}") from error
        except urllib.error.URLError:
            if attempt == max_attempts:
                raise
        time.sleep(min(2 ** attempt, 30))


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    add_connection_args(parser)
    parser.add_argument("--records", type=int, default=200)
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--source", default="rest-producer")
    args = parser.parse_args(argv)
    check_connection_args(args)

    token = TableScopedToken(args.workspace_url, workspace_id_from_endpoint(args.endpoint),
                             args.table, args.client_id, args.client_secret)
    generator = SensorEventGenerator()
    started = time.monotonic()
    sent = 0
    while sent < args.records:
        size = min(args.batch_size, args.records - sent)
        post_batch(args.endpoint, args.table, token.get(), generator.wire_batch(size, args.source))
        sent += size
        print(f"  posted {sent:,} / {args.records:,}")
    print(f"Done: {sent:,} records in {time.monotonic() - started:.1f}s")


if __name__ == "__main__":
    main()
