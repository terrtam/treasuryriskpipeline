from __future__ import annotations

import json

from src.ingestion.es_cleanup import main


class ElasticsearchResponse:
    def __init__(self, status: int, body: dict | str | None = None):
        self.status = status
        if body is None:
            self._body = b""
        elif isinstance(body, str):
            self._body = body.encode("utf-8")
        else:
            self._body = json.dumps(body).encode("utf-8")

    def read(self):
        return self._body


def test_cleanup_range_builds_delete_by_query(tmp_path, monkeypatch, capsys):
    requests = []

    def opener(req):
        requests.append(req)
        if req.method == "POST" and req.full_url.endswith("/_count"):
            return ElasticsearchResponse(200, {"count": 7})
        if req.method == "POST" and "_delete_by_query" in req.full_url:
            return ElasticsearchResponse(200, {"deleted": 7})
        raise AssertionError(f"unexpected request {req.method} {req.full_url}")

    monkeypatch.setattr("urllib.request.urlopen", opener)

    exit_code = main(
        [
            "--base-url",
            "http://localhost:9200",
            "--index",
            "treasury_audit_logs",
            "--from-date",
            "2026-06-01",
            "--to-date",
            "2026-06-07",
        ]
    )

    assert exit_code == 0
    assert [req.method for req in requests] == ["POST", "POST"]
    count_payload = json.loads(requests[0].data.decode("utf-8"))
    delete_payload = json.loads(requests[1].data.decode("utf-8"))
    assert count_payload == {
        "query": {"range": {"event_timestamp_utc": {"gte": "2026-06-01", "lte": "2026-06-07"}}}
    }
    assert delete_payload == count_payload
    output = json.loads(capsys.readouterr().out)
    assert output["deleted"] == 7
    assert output["mode"] == "range"


def test_cleanup_all_uses_match_all(monkeypatch, capsys):
    requests = []

    def opener(req):
        requests.append(req)
        if req.method == "POST" and req.full_url.endswith("/_count"):
            return ElasticsearchResponse(200, {"count": 42})
        if req.method == "POST" and "_delete_by_query" in req.full_url:
            return ElasticsearchResponse(200, {"deleted": 42})
        raise AssertionError(f"unexpected request {req.method} {req.full_url}")

    monkeypatch.setattr("urllib.request.urlopen", opener)

    exit_code = main(["--all"])

    assert exit_code == 0
    assert len(requests) == 2
    payload = json.loads(requests[0].data.decode("utf-8"))
    assert payload == {"query": {"match_all": {}}}
    output = json.loads(capsys.readouterr().out)
    assert output["deleted"] == 42
    assert output["mode"] == "all"
