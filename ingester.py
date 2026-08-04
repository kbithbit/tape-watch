"""Binance trade stream -> batched rows -> a sink (JSONL now, Stream Load in phase 2).

Every row carries three clocks so the pipeline can measure itself:

    event_ms  the exchange's clock, when the trade happened  (Binance "T")
    recv_ms   our clock, when the socket handed us the frame
    send_ms   our clock, when the batch left for the warehouse

    recv_ms - event_ms  = exchange + network lag   (not our fault)
    send_ms  - recv_ms  = our batching delay       (our fault)

All three are epoch milliseconds as plain integers. Storing them as numbers rather
than timestamps keeps the lag arithmetic exact and avoids depending on fractional-second
support in the warehouse's DATETIME type.

Clock skew, measured not assumed
--------------------------------
event_ms comes from Binance's clock; recv_ms comes from ours. Those clocks disagree.
In a 1308-trade sample, 81% of (recv_ms - event_ms) values were *negative* -- trades
appearing to arrive before they happened -- because the local clock ran ~46 ms behind
the exchange's.

So (recv_ms - event_ms) is latency PLUS an unknown constant skew, and the two cannot
be separated from one endpoint. The standard fix is a minimum filter: over a window,
the smallest observed value belongs to the fastest trade, whose true latency is near
zero, so that minimum estimates the skew. Subtracting it yields a usable *relative*
latency. That correction lives in the materialized view, where the window exists.

(send_ms - recv_ms) reads a single clock twice, so it needs no correction and is the
metric to trust. Worth knowing which of your numbers are honest.
"""

import argparse
import asyncio
import json
import random
import time
from pathlib import Path

import websockets

BINANCE_WS = "wss://stream.binance.com:9443/stream?streams={streams}"
DEFAULT_SYMBOLS = ("btcusdt", "ethusdt", "solusdt")
BATCH_ROWS = 500
BATCH_SECONDS = 1.0
MAX_BACKOFF = 30.0


def now_ms():
    return time.time_ns() // 1_000_000


def stream_url(symbols):
    return BINANCE_WS.format(streams="/".join(f"{s}@trade" for s in symbols))


def parse_frame(raw, recv_ms):
    """Combined-stream frame -> row dict, or None if it isn't a usable trade.

    Never raises: a malformed frame is a thing to count, not a thing to crash on.
    """
    try:
        msg = json.loads(raw)
    except (TypeError, ValueError):
        return None
    if not isinstance(msg, dict):
        return None
    data = msg.get("data", msg)
    if not isinstance(data, dict) or data.get("e") != "trade":
        return None
    try:
        return {
            "symbol": data["s"],
            "trade_id": int(data["t"]),
            "price": str(data["p"]),
            "qty": str(data["q"]),
            "is_buyer_maker": bool(data["m"]),
            "event_ms": int(data["T"]),
            "recv_ms": recv_ms,
        }
    except (KeyError, TypeError, ValueError):
        return None


class Batcher:
    """Buffers rows, flushes on whichever limit trips first: size or age.

    Size alone stalls on a quiet market; age alone wastes a request per trade.
    """

    def __init__(self, sink, max_rows=BATCH_ROWS, max_seconds=BATCH_SECONDS):
        self.sink = sink
        self.max_rows = max_rows
        self.max_seconds = max_seconds
        self.rows = []
        self.opened_at = None

    def add(self, row, now=None):
        now = time.monotonic() if now is None else now
        if not self.rows:
            self.opened_at = now
        self.rows.append(row)
        if len(self.rows) >= self.max_rows:
            return self.flush()
        return 0

    def is_due(self, now=None):
        if not self.rows:
            return False
        now = time.monotonic() if now is None else now
        return now - self.opened_at >= self.max_seconds

    def flush(self):
        if not self.rows:
            return 0
        rows, self.rows, self.opened_at = self.rows, [], None
        send_ms = now_ms()
        for row in rows:
            row["send_ms"] = send_ms
        self.sink(rows)
        return len(rows)


class JsonlSink:
    """Append rows as JSON lines. The development sink -- no warehouse required."""

    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def __call__(self, rows):
        with self.path.open("a") as fh:
            for row in rows:
                fh.write(json.dumps(row) + "\n")


class Stats:
    """Counters plus the rows destined for the ingest_events table."""

    def __init__(self):
        self.trades = 0
        self.malformed = 0
        self.events = []

    def event(self, kind, detail=""):
        self.events.append({"ts_ms": now_ms(), "kind": kind, "detail": str(detail)[:500]})


async def run(url, sink, seconds=None, stats=None, batch_seconds=BATCH_SECONDS):
    """Hold the socket, batch what arrives, reconnect forever with backoff."""
    stats = stats or Stats()
    batcher = Batcher(sink, max_seconds=batch_seconds)
    deadline = None if seconds is None else time.monotonic() + seconds
    backoff = 1.0

    while deadline is None or time.monotonic() < deadline:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                stats.event("connect")
                backoff = 1.0
                while deadline is None or time.monotonic() < deadline:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=batch_seconds)
                    except asyncio.TimeoutError:
                        batcher.flush()  # quiet market: age limit still applies
                        continue
                    row = parse_frame(raw, now_ms())
                    if row is None:
                        stats.malformed += 1
                        continue
                    stats.trades += 1
                    batcher.add(row)
                    if batcher.is_due():
                        batcher.flush()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - any socket failure is a reconnect
            stats.event("disconnect", f"{type(exc).__name__}: {exc}")
            batcher.flush()
            if deadline is not None and time.monotonic() >= deadline:
                break
            # ponytail: full jitter backoff, capped. Fine for one socket; add a
            # circuit breaker if this ever fans out to many streams.
            await asyncio.sleep(random.uniform(0, min(backoff, MAX_BACKOFF)))
            backoff = min(backoff * 2, MAX_BACKOFF)

    batcher.flush()
    return stats


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--out", help="directory for JSONL output")
    ap.add_argument("--seconds", type=float, help="stop after N seconds (default: forever)")
    ap.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS))
    args = ap.parse_args()

    if not args.out:
        ap.error("--out is required (--starrocks arrives in phase 2)")

    symbols = [s.strip().lower() for s in args.symbols.split(",") if s.strip()]
    path = Path(args.out) / f"live-{time.strftime('%Y%m%dT%H%M%S')}.jsonl"
    stats = asyncio.run(run(stream_url(symbols), JsonlSink(path), seconds=args.seconds))

    print(f"{path}: {stats.trades} trades, {stats.malformed} malformed")
    for event in stats.events:
        print(f"  {event['kind']}: {event['detail']}")


if __name__ == "__main__":
    main()
