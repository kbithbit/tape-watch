# tape-watch — Design

**Date:** 2026-08-04
**Status:** Approved, ready for implementation planning

## Purpose

A portfolio data-engineering project demonstrating real-time ingestion, StarRocks
modelling, and data-quality instrumentation. The deliverable is a public repo plus
a live dashboard link suitable for a CV.

Success criteria:

1. A recruiter can open one link and immediately see something live and moving.
2. A data engineer can read the repo and see real ingestion, modelling, and
   data-quality work — not a tutorial reimplementation.
3. It costs nothing to run and cannot silently die and leave a dead link.
4. It never needs StarRocks installed on the author's laptop (8 GB RAM, 14 GiB free disk).

## Non-goals

- Trading, price prediction, or financial analysis of any kind.
- Durability or exactly-once delivery. This is a demo; event loss is *measured*, not prevented.
- Historical backfill. Only live data, only a rolling window.
- Multi-exchange support. One exchange, one clean pipeline.

## Architecture

```
Binance WebSocket ──► ingester.py ──► StarRocks Stream Load ──► trades (raw)
                          │                                        │
                    3 timestamps                          async materialized view
                    stamped per event                              │
                                                          pipeline_health_1m
                                                                   │
                                                            metrics API ──► dashboard
```

Every metric in the project derives from three timestamps carried on each trade:

| Timestamp | Set by | Meaning |
|---|---|---|
| `event_ts` | Binance (`T` field) | Exchange clock — when the trade happened |
| `recv_ts` | `ingester.py` | When our socket read the frame |
| `load_ts` | StarRocks default | When it landed in the table |

`recv_ts - event_ts` is *their* latency plus the network. `load_ts - recv_ts` is
*ours*. Separating those two is the core analytical idea of the project.

### Components

| Component | Responsibility | Depends on | Approx size |
|---|---|---|---|
| `ingester.py` | Hold the WebSocket, stamp timestamps, batch ~1s, POST to Stream Load | Binance, StarRocks FE | ~120 lines |
| `schema.sql` | `trades`, `ingest_events`, `pipeline_health_1m` DDL | StarRocks | DDL only |
| `api.py` | Read endpoints over the MV; also exports the same shape to JSON | StarRocks FE | ~60 lines |
| `dashboard.html` | Single page, no build step, polls one JSON endpoint | `api.py` output | ~200 lines |
| `.github/workflows/run.yml` | The actual runtime: boot, ingest, export, publish | GitHub Actions | ~60 lines |

Each is independently testable. `ingester.py` can run with `--out fixtures/` and never
touch StarRocks. `dashboard.html` renders from a static `metrics.json` with no backend.

### Deliberate simplifications

- **No Kafka.** WebSocket → in-memory batch → Stream Load. Kafka buys replay and
  multiple consumers, neither of which this needs. Add it when a second consumer exists.
- **No orchestrator.** The async materialized view refreshes itself. No Airflow, no cron
  job, no dbt. This is also a genuine StarRocks feature rather than portable SQL.
- **No local StarRocks.** Development happens against recorded fixtures; StarRocks runs
  only in CI.

## Data model

### `trades` — landing table

```sql
CREATE TABLE trades (
    event_ts       DATETIME       NOT NULL,   -- exchange clock (Binance "T")
    symbol         VARCHAR(20)    NOT NULL,
    trade_id       BIGINT         NOT NULL,   -- monotonic per symbol
    price          DECIMAL(20,8)  NOT NULL,
    qty            DECIMAL(20,8)  NOT NULL,
    is_buyer_maker BOOLEAN        NOT NULL,   -- true => seller-initiated
    recv_ts        DATETIME(3)    NOT NULL,
    load_ts        DATETIME(3)    NOT NULL DEFAULT CURRENT_TIMESTAMP
)
DUPLICATE KEY(event_ts, symbol, trade_id)
PARTITION BY date_trunc('day', event_ts)
DISTRIBUTED BY HASH(symbol) BUCKETS 4
PROPERTIES ("replication_num" = "1");
```

Expression partitioning creates partitions on write — no dynamic-partition
configuration and no maintenance job. `replication_num = 1` because CI runs a
single BE. Raw rows are never updated or deleted.

### `ingest_events` — failure log

```sql
CREATE TABLE ingest_events (
    ts     DATETIME     NOT NULL,
    kind   VARCHAR(20)  NOT NULL,   -- 'connect' | 'disconnect' | 'error'
    detail VARCHAR(500)
)
DUPLICATE KEY(ts)
DISTRIBUTED BY HASH(ts) BUCKETS 1
PROPERTIES ("replication_num" = "1");
```

### `pipeline_health_1m` — async materialized view

```sql
CREATE MATERIALIZED VIEW pipeline_health_1m
REFRESH ASYNC EVERY (INTERVAL 1 MINUTE)
AS
SELECT
    date_trunc('minute', recv_ts)              AS minute,
    symbol,
    count(*)                                   AS events,
    percentile_approx(timestampdiff(MICROSECOND, event_ts, recv_ts)/1000, 0.50) AS ingest_lag_p50_ms,
    percentile_approx(timestampdiff(MICROSECOND, event_ts, recv_ts)/1000, 0.95) AS ingest_lag_p95_ms,
    percentile_approx(timestampdiff(MICROSECOND, recv_ts,  load_ts)/1000, 0.95) AS load_lag_p95_ms,
    max(timestampdiff(MICROSECOND, event_ts, load_ts)/1000)                     AS e2e_max_ms,
    max(trade_id) - min(trade_id) + 1 - count(DISTINCT trade_id) AS missing_events,
    sum(CASE WHEN NOT is_buyer_maker THEN qty ELSE 0 END) / nullif(sum(qty), 0)  AS buy_pressure
FROM trades
GROUP BY 1, 2;
```

**Gap detection.** Binance trade IDs are monotonic per symbol, so within a window
`max(trade_id) - min(trade_id) + 1` is the number of trades that *should* be present.
Subtracting `count(DISTINCT trade_id)` yields provable event loss. This is a plain
aggregate rather than a window function: cheaper, and unaffected by out-of-order arrival.

Known limitation, to be carried in the code as a comment:

```
-- ponytail: gap count is per-minute-window, so a trade landing either side of a
-- minute boundary reads as one missing event. Move to a window function spanning
-- windows if the false-positive rate ever matters.
```

Grouping by `recv_ts` (not `event_ts`) is deliberate: the window describes *when we
observed* the data, which is what an observability view should measure.

## Dashboard

Single HTML page, no build step, polls one JSON document.

| Panel | Scope | Source | Rationale |
|---|---|---|---|
| Run age — minutes since the last successful run | history | run metadata | Answers "is this project still alive" |
| Freshness — max `load_ts - recv_ts` observed during the run | this run | MV | In-run pipeline health |
| Latency — p50/p95 per minute, exchange-lag vs pipeline-lag split | this run | MV | Separates their slowness from ours |
| Throughput — events/sec by symbol | this run | MV | Visible movement |
| Event loss — missing count and loss rate | this run | MV | The differentiator; a provable number |
| Reconnects and dropped batches | this run | `ingest_events` | Evidence the failure path was considered |
| Buy pressure — one small gauge | this run | MV | A grab-handle for non-technical viewers |
| Trend — p95 latency and loss rate across recent runs | history | `history.json` | Shows the pipeline behaving consistently over time |

### State and history

StarRocks is a fresh container on every CI run, so the database holds only that run's
~3 minutes of data. Anything longer-lived must survive outside it.

Each run appends one summary record — timestamp, per-symbol event count, p95 latencies,
missing-event count, reconnect count — to `history.json` on the Pages branch, trimmed to
the most recent 96 records (~48 h). The dashboard reads two documents: `metrics.json`
for the current run's detail and `history.json` for the trend panels.

Every panel is labelled with its scope so nothing implies continuous coverage the
pipeline does not have. Daily partitioning on `trades` is therefore inert in CI; it is
retained because it is correct for the hosted path and costs nothing.

## Runtime and hosting

The laptop constraint (8 GB RAM, 14 GiB free disk, no Docker) is resolved by never
running StarRocks locally.

| Environment | Runs | Cost |
|---|---|---|
| Laptop | `ingester.py --out fixtures/`, pytest | ~50 MB of Python deps |
| GitHub Actions | StarRocks allin1 service container — the entire runtime | $0 (public repos have unlimited minutes) |
| GitHub Pages | `dashboard.html` + `metrics.json` | $0 |

Scheduled workflow, every 30 minutes plus manual dispatch:

1. Boot StarRocks allin1, wait for FE readiness.
2. Apply `schema.sql`.
3. Run `ingester.py` against live Binance for ~3 minutes.
4. Wait for one MV refresh.
5. Export `metrics.json` via `api.py`, and append a summary record to `history.json`.
6. Publish both to GitHub Pages.

The dashboard is therefore never more than ~30 minutes stale, has no server that can
die, and every run is public evidence the pipeline works. If always-on serving is
wanted later, point `dashboard.html` at a hosted `api.py`; nothing upstream changes.

## Error handling

| Failure | Response |
|---|---|
| WebSocket drops | Reconnect with exponential backoff; log a row to `ingest_events` |
| Stream Load returns non-`Success` | Log to `ingest_events`, drop the batch, continue. Loss is measured, not prevented |
| StarRocks unreachable at startup | CI job fails loudly — a broken pipeline must not publish a stale-but-green dashboard |
| Binance sends a malformed frame | Count it, skip it, keep the socket open |
| No trades for a symbol in a window | MV row simply absent; dashboard renders a gap rather than a zero |

The guiding rule: the ingester never crashes on bad data, but CI fails hard on broken
infrastructure.

## Testing

**Local, no StarRocks:**

- Parsing recorded Binance frames from `fixtures/`.
- Batch-boundary behaviour (flush on size, flush on time).
- Gap-detection arithmetic, including the known minute-boundary false positive.

**CI, with StarRocks:**

One end-to-end assertion: load a fixture with three deliberately removed trade IDs and
assert `missing_events = 3`. That single check validates the schema, Stream Load, MV
refresh, and the metric together.

No mocking framework, no StarRocks test double, no per-function suites.

## CV framing

> **Real-time crypto trade pipeline with self-monitoring data quality** — Binance
> WebSocket → StarRocks Stream Load → async materialized views. Tracks p95 ingest
> latency and detects provable event loss via trade-ID sequence gaps. Runs entirely
> on scheduled CI; live dashboard, $0 infrastructure.

## Open questions

None. Hosting was deferred during design and is resolved above by the scheduled-CI
approach; the `api.py` seam keeps a hosted alternative available without redesign.
