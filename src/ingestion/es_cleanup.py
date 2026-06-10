from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import date
import os
from typing import Any
from urllib import error as urllib_error
from urllib import request as urllib_request

from src.config.db_config import load_elasticsearch_config


@dataclass(frozen=True)
class CleanupResult:
    index: str
    mode: str
    deleted: int
    dry_run: bool
    query: dict[str, Any]


def _request_json(
    opener: Any,
    method: str,
    url: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    body = None
    headers = {"Content-Type": "application/json"}
    if payload is not None:
        body = json.dumps(payload, sort_keys=True).encode("utf-8")

    req = urllib_request.Request(url, data=body, headers=headers, method=method)
    try:
        response = opener(req)
        raw = response.read().decode("utf-8") if hasattr(response, "read") else ""
        if hasattr(response, "status") and response.status >= 400:
            raise RuntimeError(f"{method} {url} failed with status {response.status}: {raw}")
        return json.loads(raw) if raw else {}
    except Exception as exc:
        if isinstance(exc, urllib_error.HTTPError):
            raw = exc.read().decode("utf-8")
            raise RuntimeError(f"{method} {url} failed with status {exc.code}: {raw}") from exc
        raise


def _build_query(args: argparse.Namespace) -> dict[str, Any]:
    if args.all:
        return {"match_all": {}}

    query: dict[str, Any] = {"range": {args.field: {}}}
    field_range = query["range"][args.field]
    if args.from_date:
        field_range["gte"] = args.from_date
    if args.to_date:
        field_range["lte"] = args.to_date
    return query


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    default_config = load_elasticsearch_config()
    parser = argparse.ArgumentParser(
        description="Delete Treasury audit documents from Elasticsearch by date range or entirely."
    )
    parser.add_argument("--base-url", default=default_config.base_url, help="Elasticsearch base URL")
    parser.add_argument("--index", default=default_config.index_name, help="Elasticsearch index to clean")
    parser.add_argument(
        "--field",
        default="event_timestamp_utc",
        help="Date field to filter on when deleting a range",
    )
    parser.add_argument("--from-date", help="Inclusive start date in YYYY-MM-DD")
    parser.add_argument("--to-date", help="Inclusive end date in YYYY-MM-DD")
    parser.add_argument("--all", action="store_true", help="Delete every document in the index")
    parser.add_argument("--dry-run", action="store_true", help="Show the matching count without deleting")
    return parser.parse_args(argv)


def _validate_args(args: argparse.Namespace) -> None:
    if args.all:
        return
    if not args.from_date and not args.to_date:
        raise SystemExit("Provide either --all or at least one of --from-date / --to-date.")
    if args.from_date:
        date.fromisoformat(args.from_date)
    if args.to_date:
        date.fromisoformat(args.to_date)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    _validate_args(args)

    base_url = args.base_url.rstrip("/")
    index_url = f"{base_url}/{args.index}"
    query = _build_query(args)
    opener = urllib_request.urlopen

    count_response = _request_json(opener, "POST", f"{index_url}/_count", {"query": query})
    match_count = int(count_response.get("count", 0) or 0)

    if args.dry_run:
        print(
            json.dumps(
                CleanupResult(
                    index=args.index,
                    mode="dry-run",
                    deleted=0,
                    dry_run=True,
                    query=query,
                ).__dict__,
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    delete_response = _request_json(
        opener,
        "POST",
        f"{index_url}/_delete_by_query?conflicts=proceed&refresh=true",
        {"query": query},
    )
    deleted = int(delete_response.get("deleted", match_count) or 0)
    print(
        json.dumps(
            asdict(
                CleanupResult(
                    index=args.index,
                    mode="all" if args.all else "range",
                    deleted=deleted,
                    dry_run=False,
                    query=query,
                )
            ),
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
