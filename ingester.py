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
import hashlib
import json
import os
import random
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
import websockets

# data-stream.binance.vision, not stream.binance.com: the latter answers HTTP 451
# (geo-blocked) from GitHub's US-based runners, while working fine from a laptop
# elsewhere. Same market, same message format, no such restriction.
WS_HOST = os.getenv("TAPEWATCH_WS_HOST", "wss://data-stream.binance.vision")
DEFAULT_SYMBOLS = ("btcusdt", "ethusdt", "solusdt")
BATCH_ROWS = 500
BATCH_SECONDS = 1.0
MAX_BACKOFF = 30.0


def now_ms():
    return time.time_ns() // 1_000_000


def stream_url(symbols):
    return f'{WS_HOST}/stream?streams={"/".join(f"{s}@trade" for s in symbols)}'


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
        event_ms = int(data["T"])
        return {
            # event_ts is derived here, not in a Stream Load expression: the partition
            # key is worth a duplicated field to keep the load definition trivial.
            "event_ts": datetime.fromtimestamp(event_ms / 1000, timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            "symbol": data["s"],
            "trade_id": int(data["t"]),
            "price": str(data["p"]),
            "qty": str(data["q"]),
            "is_buyer_maker": bool(data["m"]),
            "event_ms": event_ms,
            "recv_ms": recv_ms,
        }
    except (KeyError, TypeError, ValueError, OSError, OverflowError):
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
            row.setdefault("send_ms", send_ms)  # replayed rows keep their original clock
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
        self.dropped = 0
        self.events = []

    def event(self, kind, detail=""):
        self.events.append(
            {
                "ts": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "kind": kind,
                "detail": str(detail)[:500],
            }
        )


class StreamLoadSink:
    """POST a batch to StarRocks Stream Load.

    The label is a hash of the payload, so retrying an identical batch reuses the label
    and StarRocks rejects it as already-loaded. Content-addressed idempotency: no state
    to track, and a retry after an ambiguous timeout cannot double-load.
    """

    OK = ("Success", "Label Already Exists")

    # A column's DEFAULT is not applied to a JSON load that omits the key -- the row is
    # rejected as NULL instead. Naming load_ts here as a derived column is what actually
    # stamps it, and keeps it the *server's* clock rather than another of ours.
    COLUMNS = {
        "trades": (
            "event_ts,symbol,trade_id,price,qty,is_buyer_maker,"
            "event_ms,recv_ms,send_ms,load_ts=current_timestamp()"
        ),
        "ingest_events": None,  # payload already carries every column
    }

    def __init__(self, host, table="trades", db="tapewatch", auth=("root", ""), stats=None):
        self.url = f"{host.rstrip('/')}/api/{db}/{table}/_stream_load"
        self.auth = auth
        self.stats = stats or Stats()
        self.columns = self.COLUMNS.get(table)
        self.loaded = 0

    def __call__(self, rows):
        payload = json.dumps(rows).encode()
        headers = {
            "Expect": "100-continue",
            "format": "json",
            "strip_outer_array": "true",
            # Stream Load defaults this to Asia/Shanghai, so load_ts=current_timestamp()
            # gets stamped 8 hours ahead of a UTC query session and freshness reads
            # negative. Never inherit a timezone default -- state it.
            "timezone": "UTC",
            "label": "tw-" + hashlib.sha1(payload).hexdigest(),
        }
        if self.columns:
            headers["columns"] = self.columns
        try:
            resp = requests.put(
                self.url, data=payload, headers=headers, auth=self.auth,
                allow_redirects=False, timeout=60,
            )
            # The FE answers 307 with the BE that should take the write. requests drops
            # the Authorization header across hosts on redirect, so re-send it by hand.
            if resp.status_code in (307, 308):
                resp = requests.put(
                    resp.headers["Location"], data=payload, headers=headers,
                    auth=self.auth, allow_redirects=False, timeout=60,
                )
            body = resp.json()
        except Exception as exc:  # noqa: BLE001 - a failed load is data to record
            self.stats.event("load_error", f"{type(exc).__name__}: {exc}")
            self.stats.dropped += len(rows)
            return

        if body.get("Status") not in self.OK:
            self.stats.event("load_error", self._explain(body))
            self.stats.dropped += len(rows)
            return
        self.loaded += body.get("NumberLoadedRows", len(rows))

    @staticmethod
    def _explain(body):
        """Summarise a rejection, following ErrorURL for the per-row reason.

        StarRocks answers a rejected load with a generic message ("too many filtered
        rows") and a URL holding the actual per-row cause. A loader that logs only the
        generic half leaves you guessing at which column failed to convert.
        """
        parts = [
            f"{body.get('Status')}: {body.get('Message', '')}",
            f"loaded={body.get('NumberLoadedRows')} filtered={body.get('NumberFilteredRows')}",
        ]
        url = body.get("ErrorURL")
        if url:
            try:
                parts.append(requests.get(url, timeout=15).text.strip()[:400])
            except Exception as exc:  # noqa: BLE001 - diagnostics must not mask the error
                parts.append(f"(ErrorURL {url} unreadable: {exc})")
        return " | ".join(parts)


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


def replay(path, sink, stats=None):
    """Push a recorded JSONL capture through the same batching and sink path.

    Lets CI exercise the real load path against a fixture whose expected answer is
    known -- something a live market can never give you.
    """
    stats = stats or Stats()
    batcher = Batcher(sink, max_seconds=float("inf"))
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            batcher.add(json.loads(line), now=0.0)
            stats.trades += 1
    batcher.flush()
    return stats


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--out", help="write JSONL to this directory")
    ap.add_argument("--starrocks", help="FE http address, e.g. http://localhost:8030")
    ap.add_argument("--replay", help="load this JSONL file instead of the live socket")
    ap.add_argument("--seconds", type=float, help="stop after N seconds (default: forever)")
    ap.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS))
    args = ap.parse_args()

    if bool(args.out) == bool(args.starrocks):
        ap.error("give exactly one of --out or --starrocks")

    stats = Stats()
    if args.starrocks:
        sink = StreamLoadSink(args.starrocks, stats=stats)
    else:
        sink = JsonlSink(Path(args.out) / f"live-{time.strftime('%Y%m%dT%H%M%S')}.jsonl")

    if args.replay:
        replay(args.replay, sink, stats)
    else:
        symbols = [s.strip().lower() for s in args.symbols.split(",") if s.strip()]
        asyncio.run(run(stream_url(symbols), sink, seconds=args.seconds, stats=stats))

    if args.starrocks and stats.events:
        StreamLoadSink(args.starrocks, table="ingest_events", stats=Stats())(stats.events)

    loaded = getattr(sink, "loaded", stats.trades)
    print(f"{stats.trades} trades, {loaded} loaded, {stats.dropped} dropped, "
          f"{stats.malformed} malformed")
    for event in stats.events:
        print(f"  {event['kind']}: {event['detail']}")
    return 1 if stats.dropped else 0


if __name__ == "__main__":
    raise SystemExit(main())
