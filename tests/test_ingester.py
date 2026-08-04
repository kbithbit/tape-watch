"""Offline tests: no network, no StarRocks. Runs on the laptop in under a second."""

import json

from ingester import Batcher, JsonlSink, parse_frame, stream_url

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
