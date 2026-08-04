# Phase 5 — Scheduling and deployment

What we built: [.github/workflows/run.yml](../../.github/workflows/run.yml) — the entire
runtime. Every 30 minutes GitHub hands us a machine, we boot a warehouse on it, ingest
live market data for 3 minutes, publish, and the machine is destroyed.

Live: **https://kbithbit.github.io/tape-watch/** — 7,998 trades, 0 lost, on the first run.

---

## Topics covered

1. Ephemeral infrastructure and where state goes
2. The geo-blocking failure — environment parity
3. Publish gates: failing safe
4. Concurrency and the lost-update race
5. Idempotency at the job level
6. What this costs, and what it would cost differently

---

## 1. Ephemeral infrastructure

The warehouse exists for four minutes and is then destroyed. Nothing survives it.

That forces one question to the surface that long-lived infrastructure lets you ignore:
**what is state, and where does it live?**

Here the answer is `history.json`. Each run reads the previously published file back off
the live site, appends its own summary, trims to 96 records, and republishes:

```yaml
curl -fsS "$PAGES_URL/history.json" -o site/history.json || echo "starting a new series"
```

The published artifact *is* the database for cross-run state. That sounds like a hack, and
at scale it would be — but it has real properties: no credentials, no extra service, and
it fails to an empty series rather than an error.

> **Q: What breaks first if this grew?**
>
> Read-modify-write on a whole file has no concurrency control beyond the workflow's own
> lock, and the file grows linearly. At 96 records it's a few kilobytes. If runs became
> frequent or parallel I'd move to something with atomic appends — but I'd move because a
> specific limit was reached, not preemptively.

> **Q: Isn't destroying the warehouse every run wasteful?**
>
> It costs about a minute of boot time per run. In exchange every run starts from a known
> empty state, so there is no possibility of stale data or accumulated drift explaining a
> result. That's the same argument as immutable infrastructure generally: pay a fixed
> setup cost to eliminate a class of "works on the old one" bugs.

---

## 2. The geo-blocking failure — the lesson of this phase

The first scheduled run ingested **zero trades**. The ingester's own log said why:

```
disconnect: InvalidStatus: server rejected WebSocket connection: HTTP 451
```

**451 — Unavailable For Legal Reasons.** Binance geo-blocks GitHub's US-based runners.
The exact same code had worked on a laptop hundreds of times.

This is environment parity, and it's the classic version of it: **the code was never the
variable — the network position was.** Local success proved the code worked *from where I
was standing*, which is a weaker claim than it feels like.

The fix came from probing rather than guessing. I wrote a throwaway workflow that opened
six public feeds from an actual runner and reported which answered:

| feed | result |
|---|---|
| `stream.binance.com` | FAIL — HTTP 451 |
| `data-stream.binance.vision` | **OK — identical format, same market** |
| `stream.binance.us` | OK — different market (US venue) |
| coinbase / kraken / bitstamp | OK — different message formats |

`data-stream.binance.vision` is Binance's public market-data domain: same trade stream,
same message schema, no geo-restriction. **One line changed.** The fixture, the schema,
the gap metric and every doc stayed valid.

> **Q: Why probe instead of just switching to Coinbase?**
>
> Because switching exchanges would have invalidated the fixture, the gap metric's
> assumption about monotonic trade IDs, and four documents — a large change to fix a
> problem I hadn't diagnosed yet. Twenty lines of probe bought the information that made
> the fix one line. **Guessing is only cheaper than measuring when you guess right.**

> **Q: How would you catch this class of thing earlier?**
>
> Run the real thing in the target environment as early as possible. Everything in phases
> 1–4 was verified in CI, so it worked. This step was the one part whose environment I'd
> only ever exercised locally — and it's the one that broke.

---

## 3. Failing safe

`export.py --min-events 100` exits non-zero if a run ingested almost nothing.

The reasoning is about **which failure is worse.** A run that publishes "0 events, 0 lost,
everything healthy" is far more damaging than a run that publishes nothing: the first is
convincingly wrong, the second leaves yesterday's correct dashboard up and turns the badge
red. Failing loudly is a feature.

That gate fired on the very first scheduled run and stopped the geo-block from being
published as a clean bill of health. It paid for itself immediately.

> **Q: How did you choose 100?**
>
> Arbitrarily, and I'd say so. It's an order of magnitude below a normal run (~8,000) and
> an order above zero, so it separates "broken" from "quiet market" without pretending to
> a precision I don't have. A threshold you can't justify should at least be one you can
> explain.

---

## 4. Concurrency

```yaml
concurrency:
  group: pages
  cancel-in-progress: false
```

Two overlapping runs would both read the same `history.json`, both append, and both
publish — the second overwriting the first. A **lost update**, the same race as any
unguarded read-modify-write.

`cancel-in-progress: false` is the deliberate half. The GitHub default for a `pages` group
is often to cancel the older run, which is right when only the newest deploy matters. Here
a cancelled run publishes *nothing* and its 3 minutes of ingested data are lost, so
queueing is correct.

---

## 5. Idempotency at the job level

Phase 2 made the *load* idempotent with content-addressed labels. This phase makes the
*job* safe to re-run: it can be triggered manually at any time, and because each run starts
from an empty warehouse and appends exactly one history record, re-running produces one more
record rather than corrupting anything.

The one non-idempotent part is honest to name: re-running appends a *duplicate-ish* history
record, since it's a new run over new data. That's correct behaviour rather than a bug —
runs are events, not states.

---

## 6. Cost

**$0.** Public repositories get unlimited GitHub Actions minutes, and Pages hosting is free.
48 runs a day at ~4.5 minutes each would be roughly 6,500 minutes a month — which on a
private repo's 2,000-minute free tier would fail in about ten days.

That's not a footnote; it's why the repo is public, and it's a real engineering decision
driven by a platform constraint.

> **Q: What would you change to run this for real, 24/7?**
>
> A long-lived StarRocks with replication ≥ 3 and real retention on the raw table, an
> always-on ingester with a bounded buffer and a replayable log (Kafka) between it and the
> warehouse, and alerting on the freshness metric rather than a dashboard nobody watches.
> The pipeline logic barely changes; everything that changes is about surviving time —
> which is the honest difference between a portfolio project and production.

---

## The one-liner

> "The first scheduled run ingested nothing — Binance returns HTTP 451 to GitHub's US
> runners, though it works fine from my laptop. Rather than guess, I probed six feeds from
> an actual runner and found one with the identical message format, so the fix was one
> line. My publish gate had already refused to publish the empty run."
