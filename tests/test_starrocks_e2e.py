"""End-to-end test against a real StarRocks. Skipped unless STARROCKS_E2E=1.

fixtures/gaps.jsonl is a real 1-minute capture with three ETHUSDT trades deliberately
removed from the middle of the sequence, and BTCUSDT left intact. A live market can
never tell you the right answer; a doctored fixture can.
"""

import os

import pytest

import sr

pytestmark = pytest.mark.skipif(
    os.getenv("STARROCKS_E2E") != "1", reason="needs a running StarRocks"
)

EXPECTED_MISSING = {"ETHUSDT": 3, "BTCUSDT": 0}
EXPECTED_ROWS = {"ETHUSDT": 602, "BTCUSDT": 569}


@pytest.fixture(scope="module")
def gaps():
    rows = sr.query(
        """
        SELECT symbol,
               count(*)                                                     AS rows_loaded,
               max(trade_id) - min(trade_id) + 1 - count(DISTINCT trade_id) AS missing
        FROM tapewatch.trades
        GROUP BY symbol
        """
    )
    return {r["symbol"]: r for r in rows}


def test_both_symbols_loaded(gaps):
    assert set(gaps) == set(EXPECTED_ROWS)


def test_row_counts_match_the_fixture(gaps):
    assert {s: int(r["rows_loaded"]) for s, r in gaps.items()} == EXPECTED_ROWS


def test_gap_detection_finds_exactly_the_removed_trades(gaps):
    """The headline metric: provable event loss, not an estimate."""
    assert {s: int(r["missing"]) for s, r in gaps.items()} == EXPECTED_MISSING


def test_server_stamped_load_ts(gaps):
    """load_ts has no value in the payload -- StarRocks must supply it."""
    (row,) = sr.query("SELECT count(*) AS n FROM tapewatch.trades WHERE load_ts IS NULL")
    assert int(row["n"]) == 0


def test_clocks_survived_the_round_trip(gaps):
    """recv_ms/send_ms must arrive as integers, or every latency metric is silently wrong."""
    (row,) = sr.query(
        "SELECT min(send_ms - recv_ms) AS lo, max(send_ms - recv_ms) AS hi "
        "FROM tapewatch.trades"
    )
    assert int(row["lo"]) >= 0, "batch cannot be sent before it was received"
    assert int(row["hi"]) < 60_000, "replayed batching delay should be well under a minute"


def test_reload_is_idempotent():
    """Same payload, same content hash, same label -- StarRocks must refuse the second load."""
    import ingester

    before = sr.query("SELECT count(*) AS n FROM tapewatch.trades")[0]["n"]
    stats = ingester.Stats()
    sink = ingester.StreamLoadSink(os.environ["STARROCKS_FE"], stats=stats)
    ingester.replay("fixtures/gaps.jsonl", sink, stats)
    after = sr.query("SELECT count(*) AS n FROM tapewatch.trades")[0]["n"]

    assert int(after) == int(before), "duplicate label must not double-load"
    assert stats.dropped == 0, "a rejected duplicate is success, not a drop"
