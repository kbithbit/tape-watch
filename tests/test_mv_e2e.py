"""End-to-end checks on the materialized view and the exported JSON.

Runs after export.py in CI. Skipped unless STARROCKS_E2E=1.
"""

import json
import os
from pathlib import Path

import pytest

import sr

pytestmark = pytest.mark.skipif(
    os.getenv("STARROCKS_E2E") != "1", reason="needs a running StarRocks"
)

SITE = Path(os.getenv("SITE_DIR", "site"))


@pytest.fixture(scope="module")
def rows():
    return {r["symbol"]: r for r in sr.query("SELECT * FROM tapewatch.pipeline_health_1m")}


@pytest.fixture(scope="module")
def metrics():
    return json.loads((SITE / "metrics.json").read_text())


def test_mv_refreshed_and_covers_both_symbols(rows):
    assert set(rows) == {"BTCUSDT", "ETHUSDT"}


def test_mv_reproduces_the_gap_metric(rows):
    """Same answer as the raw-table query, now precomputed."""
    assert int(rows["ETHUSDT"]["missing_events"]) == 3
    assert int(rows["BTCUSDT"]["missing_events"]) == 0


def test_raw_cross_clock_lag_is_negative_as_captured(rows):
    """The fixture was recorded on a clock running behind Binance's.

    If this ever goes positive the fixture was regenerated on a synced machine -- the
    skew correction is still correct, but this test is no longer demonstrating anything.
    """
    assert int(rows["BTCUSDT"]["skew_est_ms"]) < 0


def test_skew_correction_cannot_produce_negative_latency(rows):
    """The whole point of subtracting the window minimum."""
    for symbol, row in rows.items():
        assert float(row["ingest_lag_p50_ms"]) >= 0, symbol
        assert float(row["ingest_lag_p95_ms"]) >= 0, symbol


def test_corrected_lag_is_ordered_p50_then_p95(rows):
    for symbol, row in rows.items():
        assert float(row["ingest_lag_p50_ms"]) <= float(row["ingest_lag_p95_ms"]), symbol


def test_single_clock_metric_needs_no_correction(rows):
    """send_ms - recv_ms reads one clock twice, so it is non-negative by construction."""
    for symbol, row in rows.items():
        assert float(row["buffer_lag_p50_ms"]) >= 0, symbol


def test_buy_pressure_is_a_fraction(rows):
    for symbol, row in rows.items():
        assert 0.0 <= float(row["buy_pressure"]) <= 1.0, symbol


def test_metrics_json_has_what_the_dashboard_reads(metrics):
    assert set(metrics) == {"summary", "freshness", "minutes", "ingest_events"}
    assert metrics["summary"]["events"] == 1171
    assert metrics["summary"]["missing_events"] == 3
    assert metrics["minutes"], "dashboard needs per-minute rows to draw anything"


def test_history_starts_with_this_run():
    history = json.loads((SITE / "history.json").read_text())
    assert len(history) == 1
    assert history[0]["events"] == 1171


def test_loss_rate_matches_the_fixture():
    """3 missing out of 1174 expected."""
    metrics = json.loads((SITE / "metrics.json").read_text())
    assert metrics["summary"]["loss_rate"] == pytest.approx(3 / 1174, rel=1e-9)
