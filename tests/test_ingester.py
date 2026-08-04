"""Offline tests: no network, no StarRocks. Runs on the laptop in under a second."""

import json

import ingester
from ingester import Batcher, JsonlSink, Stats, StreamLoadSink, parse_frame, stream_url


class FakeResponse:
    def __init__(self, status_code=200, body=None, location=None):
        self.status_code = status_code
        self._body = body or {"Status": "Success", "NumberLoadedRows": 2}
        self.headers = {"Location": location} if location else {}

    def json(self):
        return self._body


def fake_put(monkeypatch, *responses):
    """Queue responses for successive requests.put calls; record what was sent."""
    calls = []

    def _put(url, **kwargs):
        calls.append({"url": url, **kwargs})
        return responses[min(len(calls) - 1, len(responses) - 1)]

    monkeypatch.setattr(ingester.requests, "put", _put)
    return calls

FRAME = json.dumps(
    {
        "stream": "btcusdt@trade",
        "data": {
            "e": "trade",
            "E": 1754320200150,
            "s": "BTCUSDT",
            "t": 4242,
            "p": "61234.50",
            "q": "0.015",
            "T": 1754320200100,
            "m": True,
            "M": True,
        },
    }
)


def test_parse_frame_keeps_both_clocks():
    row = parse_frame(FRAME, recv_ms=1754320200180)
    assert row["symbol"] == "BTCUSDT"
    assert row["trade_id"] == 4242
    assert row["is_buyer_maker"] is True
    assert row["event_ms"] == 1754320200100
    assert row["recv_ms"] == 1754320200180
    # the metric the whole project hangs on
    assert row["recv_ms"] - row["event_ms"] == 80


def test_parse_frame_rejects_junk_without_raising():
    for raw in ["", "not json", "[]", "null", json.dumps({"data": {"e": "depthUpdate"}})]:
        assert parse_frame(raw, 0) is None


def test_parse_frame_rejects_trade_missing_fields():
    broken = json.dumps({"data": {"e": "trade", "s": "BTCUSDT"}})
    assert parse_frame(broken, 0) is None


def test_batcher_flushes_on_size():
    flushed = []
    batcher = Batcher(flushed.append, max_rows=3, max_seconds=999)
    for i in range(5):
        batcher.add({"trade_id": i}, now=0.0)
    assert [len(b) for b in flushed] == [3]
    assert len(batcher.rows) == 2  # remainder still buffered


def test_batcher_flushes_on_age():
    flushed = []
    batcher = Batcher(flushed.append, max_rows=999, max_seconds=1.0)
    batcher.add({"trade_id": 1}, now=100.0)
    assert not batcher.is_due(now=100.5)
    assert batcher.is_due(now=101.0)
    batcher.flush()
    assert [len(b) for b in flushed] == [1]


def test_batcher_stamps_send_ms_on_every_row():
    flushed = []
    batcher = Batcher(flushed.append, max_rows=2, max_seconds=999)
    batcher.add({"trade_id": 1}, now=0.0)
    batcher.add({"trade_id": 2}, now=0.0)
    assert all("send_ms" in row for row in flushed[0])
    assert flushed[0][0]["send_ms"] == flushed[0][1]["send_ms"]


def test_batcher_flush_when_empty_is_a_noop():
    flushed = []
    Batcher(flushed.append).flush()
    assert flushed == []


def test_jsonl_sink_appends(tmp_path):
    sink = JsonlSink(tmp_path / "nested" / "out.jsonl")
    sink([{"trade_id": 1}, {"trade_id": 2}])
    sink([{"trade_id": 3}])
    lines = (tmp_path / "nested" / "out.jsonl").read_text().strip().split("\n")
    assert [json.loads(line)["trade_id"] for line in lines] == [1, 2, 3]


def test_stream_url_builds_combined_stream():
    assert stream_url(["btcusdt", "ethusdt"]).endswith(
        "streams=btcusdt@trade/ethusdt@trade"
    )


ROWS = [{"trade_id": 1, "symbol": "BTCUSDT"}, {"trade_id": 2, "symbol": "BTCUSDT"}]


def test_label_is_content_addressed(monkeypatch):
    """Same rows must produce the same label, or retries double-load."""
    calls = fake_put(monkeypatch, FakeResponse())
    StreamLoadSink("http://fe:8030", stats=Stats())(ROWS)
    StreamLoadSink("http://fe:8030", stats=Stats())(list(ROWS))
    assert calls[0]["headers"]["label"] == calls[1]["headers"]["label"]

    StreamLoadSink("http://fe:8030", stats=Stats())(ROWS + [{"trade_id": 3}])
    assert calls[2]["headers"]["label"] != calls[0]["headers"]["label"]


def test_redirect_is_followed_with_auth_reattached(monkeypatch):
    """requests drops Authorization across hosts, so the retry must carry it explicitly."""
    calls = fake_put(
        monkeypatch,
        FakeResponse(307, location="http://be:8040/api/tapewatch/trades/_stream_load"),
        FakeResponse(),
    )
    sink = StreamLoadSink("http://fe:8030", stats=Stats())
    sink(ROWS)

    assert calls[0]["url"].startswith("http://fe:8030")
    assert calls[1]["url"].startswith("http://be:8040")
    assert calls[1]["auth"] == ("root", "")
    assert calls[1]["headers"]["label"] == calls[0]["headers"]["label"]
    assert sink.loaded == 2


def test_trades_load_names_load_ts_as_a_derived_column(monkeypatch):
    """Omit load_ts and StarRocks rejects the row as NULL -- its DEFAULT does not apply."""
    calls = fake_put(monkeypatch, FakeResponse())
    StreamLoadSink("http://fe:8030", table="trades", stats=Stats())(ROWS)
    assert "load_ts=current_timestamp()" in calls[0]["headers"]["columns"]


def test_load_pins_the_timezone(monkeypatch):
    """Stream Load defaults to Asia/Shanghai; inheriting that made freshness negative."""
    calls = fake_put(monkeypatch, FakeResponse())
    StreamLoadSink("http://fe:8030", stats=Stats())(ROWS)
    assert calls[0]["headers"]["timezone"] == "UTC"


def test_ingest_events_load_sends_no_column_mapping(monkeypatch):
    calls = fake_put(monkeypatch, FakeResponse())
    StreamLoadSink("http://fe:8030", table="ingest_events", stats=Stats())(ROWS)
    assert "columns" not in calls[0]["headers"]


def test_duplicate_label_is_success_not_a_drop(monkeypatch):
    fake_put(monkeypatch, FakeResponse(body={"Status": "Label Already Exists"}))
    stats = Stats()
    StreamLoadSink("http://fe:8030", stats=stats)(ROWS)
    assert stats.dropped == 0
    assert stats.events == []


def test_failed_load_is_counted_not_raised(monkeypatch):
    fake_put(monkeypatch, FakeResponse(body={"Status": "Fail", "Message": "boom"}))
    stats = Stats()
    StreamLoadSink("http://fe:8030", stats=stats)(ROWS)
    assert stats.dropped == 2
    assert stats.events[0]["kind"] == "load_error"
    assert "boom" in stats.events[0]["detail"]


def test_network_error_is_counted_not_raised(monkeypatch):
    def explode(url, **kwargs):
        raise ConnectionError("no route to host")

    monkeypatch.setattr(ingester.requests, "put", explode)
    stats = Stats()
    StreamLoadSink("http://fe:8030", stats=stats)(ROWS)
    assert stats.dropped == 2
    assert "no route to host" in stats.events[0]["detail"]


def test_replay_preserves_original_send_ms(tmp_path):
    """Replayed rows keep the clocks they were captured with, or the fixture lies."""
    path = tmp_path / "capture.jsonl"
    path.write_text(
        json.dumps({"trade_id": 1, "recv_ms": 100, "send_ms": 700}) + "\n\n"
        + json.dumps({"trade_id": 2, "recv_ms": 200, "send_ms": 700}) + "\n"
    )
    flushed = []
    stats = ingester.replay(path, flushed.append)
    assert stats.trades == 2
    assert [r["send_ms"] for r in flushed[0]] == [700, 700]
