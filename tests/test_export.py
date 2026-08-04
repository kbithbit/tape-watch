"""Offline tests for the pure parts of the exporter -- no StarRocks needed."""

import json
from datetime import datetime
from decimal import Decimal

from export import clean, jsonable, load_history, summarise, trim_history

MINUTES = [
    {"minute": "2026-08-04 17:25:00", "symbol": "BTCUSDT", "events": 4000,
     "missing_events": 0, "ingest_lag_p95_ms": 40, "buffer_lag_p95_ms": 900,
     "skew_est_ms": -46},
    {"minute": "2026-08-04 17:25:00", "symbol": "ETHUSDT", "events": 1000,
     "missing_events": 3, "ingest_lag_p95_ms": 95, "buffer_lag_p95_ms": 1100,
     "skew_est_ms": -44},
]
EVENTS = [
    {"ts": "2026-08-04 17:25:00", "kind": "connect", "detail": ""},
    {"ts": "2026-08-04 17:26:00", "kind": "disconnect", "detail": "ConnectionClosed"},
    {"ts": "2026-08-04 17:27:00", "kind": "load_error", "detail": "Fail"},
]


def test_jsonable_converts_what_json_cannot_hold():
    assert jsonable(Decimal("0.5")) == 0.5
    assert jsonable(datetime(2026, 8, 4, 17, 25, 3)) == "2026-08-04 17:25:03"
    assert jsonable(42) == 42
    assert jsonable(None) is None


def test_clean_round_trips_through_json():
    rows = clean([{"qty": Decimal("1.5"), "ts": datetime(2026, 8, 4, 1, 2, 3)}])
    assert json.loads(json.dumps(rows)) == [{"qty": 1.5, "ts": "2026-08-04 01:02:03"}]


def test_summarise_totals_events_and_losses():
    s = summarise(MINUTES, EVENTS)
    assert s["events"] == 5000
    assert s["missing_events"] == 3
    assert s["symbols"] == ["BTCUSDT", "ETHUSDT"]
    assert s["minutes_covered"] == 1


def test_loss_rate_is_weighted_by_volume_not_averaged_across_minutes():
    """3 missing out of 5003 expected -- not the mean of 0% and 0.3%."""
    s = summarise(MINUTES, EVENTS)
    assert s["loss_rate"] == 3 / 5003


def test_loss_rate_is_zero_when_there_is_nothing_to_lose():
    assert summarise([], [])["loss_rate"] == 0.0


def test_summarise_reports_the_worst_minute_not_the_average():
    s = summarise(MINUTES, EVENTS)
    assert s["ingest_lag_p95_ms"] == 95
    assert s["buffer_lag_p95_ms"] == 1100


def test_summarise_counts_failures_by_kind():
    s = summarise(MINUTES, EVENTS)
    assert s["reconnects"] == 1
    assert s["load_errors"] == 1


def test_skew_estimate_takes_the_most_negative_window():
    assert summarise(MINUTES, EVENTS)["skew_est_ms"] == -46


def test_trim_history_keeps_the_newest():
    assert trim_history(list(range(200)), limit=96) == list(range(104, 200))
    assert trim_history([1, 2], limit=96) == [1, 2]
    assert trim_history([1, 2], limit=0) == []


def test_load_history_survives_a_corrupt_file(tmp_path):
    """A broken history must not stop the run that would have replaced it."""
    path = tmp_path / "history.json"
    path.write_text("{ not json")
    assert load_history(path) == []

    path.write_text('{"not": "a list"}')
    assert load_history(path) == []

    assert load_history(tmp_path / "absent.json") == []
