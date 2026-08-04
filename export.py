"""Read the health MV and write the two JSON documents the dashboard renders.

metrics.json  -- this run in detail: every minute, every symbol
history.json  -- one summary record per run, trimmed, so trends survive the warehouse

The split exists because StarRocks is thrown away at the end of every CI run. Anything
that must outlive the container has to be written down outside it.
"""

import argparse
import json
import os
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import sr

HISTORY_LIMIT = 96  # ~48 hours at one run every 30 minutes

MINUTES_SQL = """
    SELECT * FROM tapewatch.pipeline_health_1m ORDER BY minute, symbol
"""

EVENTS_SQL = """
    SELECT ts, kind, detail FROM tapewatch.ingest_events ORDER BY ts
"""

FRESHNESS_SQL = """
    SELECT timestampdiff(SECOND, max(load_ts), now()) AS staleness_s,
           max(load_ts)                               AS last_load_ts
    FROM tapewatch.trades
"""


def jsonable(value):
    """pymysql hands back Decimal and datetime; JSON knows neither."""
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return value


def clean(rows):
    return [{k: jsonable(v) for k, v in row.items()} for row in rows]


def summarise(minutes, events):
    """Collapse per-minute rows into the one record that goes into history.

    Loss rate is computed from totals rather than averaged across minutes: a minute
    with 4 events and a minute with 4000 are not equally informative, and averaging
    their rates would pretend they are.
    """
    total_events = sum(m["events"] for m in minutes)
    total_missing = sum(m["missing_events"] for m in minutes)
    expected = total_events + total_missing

    def worst(field):
        return max((m[field] for m in minutes), default=None)

    return {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "run_id": os.getenv("GITHUB_RUN_ID", "local"),
        "run_url": (
            f"{os.getenv('GITHUB_SERVER_URL', 'https://github.com')}/"
            f"{os.getenv('GITHUB_REPOSITORY', '')}/actions/runs/{os.getenv('GITHUB_RUN_ID', '')}"
            if os.getenv("GITHUB_RUN_ID")
            else None
        ),
        "minutes_covered": len({m["minute"] for m in minutes}),
        "symbols": sorted({m["symbol"] for m in minutes}),
        "events": total_events,
        "missing_events": total_missing,
        "loss_rate": (total_missing / expected) if expected else 0.0,
        "ingest_lag_p95_ms": worst("ingest_lag_p95_ms"),
        "buffer_lag_p95_ms": worst("buffer_lag_p95_ms"),
        "skew_est_ms": min((m["skew_est_ms"] for m in minutes), default=None),
        "reconnects": sum(1 for e in events if e["kind"] == "disconnect"),
        "load_errors": sum(1 for e in events if e["kind"] == "load_error"),
    }


def trim_history(records, limit=HISTORY_LIMIT):
    """Keep the newest `limit` records. Unbounded history is a slow-motion outage."""
    return records[-limit:] if limit > 0 else []


def load_history(path):
    if not Path(path).exists():
        return []
    try:
        records = json.loads(Path(path).read_text())
        return records if isinstance(records, list) else []
    except json.JSONDecodeError:
        # A corrupt history must not take down the run that would have replaced it.
        return []


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--out", default="site", help="directory to write JSON into")
    ap.add_argument("--refresh", action="store_true", help="force a synchronous MV refresh first")
    ap.add_argument("--min-events", type=int, default=0,
                    help="fail instead of publishing a run that ingested fewer than this")
    args = ap.parse_args()

    if args.refresh:
        # Waiting out the async schedule would cost a minute of CI for no benefit.
        sr.query("REFRESH MATERIALIZED VIEW tapewatch.pipeline_health_1m WITH SYNC MODE")

    minutes = clean(sr.query(MINUTES_SQL))
    events = clean(sr.query(EVENTS_SQL))
    freshness = clean(sr.query(FRESHNESS_SQL))[0]
    summary = summarise(minutes, events)

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    (out / "metrics.json").write_text(
        json.dumps({"summary": summary, "freshness": freshness,
                    "minutes": minutes, "ingest_events": events}, indent=1)
    )

    history_path = out / "history.json"
    history = trim_history(load_history(history_path) + [summary])
    history_path.write_text(json.dumps(history, indent=1))

    print(
        f"{summary['events']} events over {summary['minutes_covered']} minute(s), "
        f"{summary['missing_events']} missing "
        f"({summary['loss_rate'] * 100:.3f}%), history={len(history)} runs"
    )

    # Publishing an empty run would replace a working dashboard with a broken-looking
    # one. Failing here leaves the previous publish in place, which is the safer state.
    if summary["events"] < args.min_events:
        raise SystemExit(
            f"only {summary['events']} events, below the --min-events floor of "
            f"{args.min_events}; refusing to publish"
        )


if __name__ == "__main__":
    main()
