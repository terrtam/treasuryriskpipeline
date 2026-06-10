from __future__ import annotations

import argparse
import json
from urllib import error, request


def _request_json(method: str, url: str, payload: dict | None = None) -> tuple[int, dict]:
    data = None
    headers = {"Content-Type": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")

    req = request.Request(url, data=data, headers=headers, method=method)
    try:
        with request.urlopen(req) as response:
            body = response.read().decode("utf-8")
            return response.status, json.loads(body) if body else {}
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8")
        raise RuntimeError(f"{method} {url} failed with {exc.code}: {body}") from exc


def main() -> int:
    parser = argparse.ArgumentParser(description="Simple Elasticsearch update + retrieve smoke test.")
    parser.add_argument("--base-url", default="http://localhost:9200", help="Elasticsearch base URL")
    parser.add_argument("--index", default="treasury_audit_logs", help="Index to use for the smoke test")
    parser.add_argument("--doc-id", default="smoke-test-1", help="Document ID to update and fetch")
    args = parser.parse_args()

    base_url = args.base_url.rstrip("/")
    index_url = f"{base_url}/{args.index}"
    doc_url = f"{index_url}/_doc/{args.doc_id}"
    update_url = f"{index_url}/_update/{args.doc_id}"

    # Create or replace the starting document.
    _request_json(
        "PUT",
        doc_url,
        {
            "transaction_id": args.doc_id,
            "event_type": "smoke_test",
            "status": "created",
            "counter": 1,
        },
    )

    # Update one field in place.
    _request_json(
        "POST",
        update_url,
        {
            "doc": {
                "status": "updated",
                "counter": 2,
            }
        },
    )

    # Retrieve the final document.
    _, retrieved = _request_json("GET", doc_url)
    print(json.dumps(retrieved.get("_source", retrieved), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
