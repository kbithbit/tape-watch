# Phase 3 — Materialized views and aggregation

What we built: [sql/mv.sql](../../sql/mv.sql) (the async health rollup) and
[export.py](../../export.py) (queries it, writes `metrics.json` and appends `history.json`).

Real output from CI, on the 1171-trade fixture:

| symbol | events | skew_est | raw lag p50 | **corrected p50** | **corrected p95** | buffer p50 | buffer p95 | missing |
|---|---|---|---|---|---|---|---|---|
| BTCUSDT | 569 | −46 ms | **−39 ms** | 7 ms | 97 ms | 709 ms | 1433 ms | 0 |
| ETHUSDT | 602 | −46 ms | **−34 ms** | 12 ms | 97 ms | 305 ms | 1360 ms | 3 |

The raw p50 being *negative* is the headline. The corrected 7–12 ms matches independent
laptop measurement, which is the evidence the correction is sound rather than convenient.

---

## Topics covered

1. Synchronous vs asynchronous materialized views
2. Transparent query rewrite
3. `percentile_approx` and why approximation is fine
4. Why p95, not an average
5. Refresh semantics and staleness
6. Aggregate late
7. Correcting clock skew in SQL
8. Weighting a rate correctly

---

## 1. Sync vs async materialized views

StarRocks has two kinds, and confusing them is a common interview stumble.

| | **Synchronous** | **Asynchronous** |
|---|---|---|
| Updated | On load, in the same transaction | On a schedule, or manually |
| Scope | One base table | Multiple tables, joins, subqueries |
| Freshness | Always current | Stale by up to one refresh interval |
| Cost | Paid on every write | Paid per refresh |
| Functions | Restricted set | Effectively the full query language |

We use **asynchronous**, for two reasons:

1. `percentile_approx` and `count(DISTINCT ...)` aren't available to a sync MV. The metrics
   we want need the full engine.
2. A sync MV taxes every single load. Our writes arrive once a second and are latency-
   sensitive; the read happens once per run. Paying on the rare operation rather than the
   frequent one is the right trade.

> **Q: What does the async choice cost you?**
>
> Staleness — the view can be up to a refresh interval behind. That's acceptable because
> the dashboard is a health summary, not an alerting path. If it drove alerts, a minute of
> blindness on a pipeline outage would be too long, and I'd query the base table directly
> for the freshness signal while keeping the MV for the expensive percentiles.

> **Q: This is orchestration. Where's your scheduler?**
>
> There isn't one, deliberately. `REFRESH ASYNC EVERY (INTERVAL 1 MINUTE)` puts the
> refresh inside the warehouse, which is the component that actually knows when the data
> changed. An external scheduler would need to duplicate that knowledge and would drift
> from it.
>
> Airflow earns its keep when you have cross-system dependencies, retries and backfills to
> coordinate. For "keep one aggregate current in one warehouse," it's a second system to
> operate for no gain. CI does force a synchronous refresh with
> `REFRESH MATERIALIZED VIEW ... WITH SYNC MODE`, so tests don't wait out the interval.

---

## 2. Transparent query rewrite

The feature worth knowing even though we don't rely on it: StarRocks can **rewrite a query
against the base table to use a matching materialized view**, automatically. Query `trades`
with a compatible aggregation and the optimiser may silently answer from
`pipeline_health_1m` instead.

Why that's a big deal: consumers don't have to know the MV exists. No dashboards to update,
no queries to rewrite, no coordination. You add an MV and things get faster.

> **Q: If rewrite is automatic, why does `export.py` query the MV by name?**
>
> Determinism. Rewrite is an optimisation, and optimisations are allowed to not happen —
> a slightly different predicate and you're scanning raw rows. For a scheduled job I want
> the same plan every time. Rewrite is a gift for ad-hoc users, not something a production
> job should depend on.

---

## 3. `percentile_approx`

Exact percentiles require holding every value to sort them — memory proportional to row
count. `percentile_approx` maintains a bounded sketch instead: fixed memory, small
bounded error.

The trade is nearly always worth it. Knowing p95 is "about 97 ms" rather than exactly
96.8 ms changes no decision anyone makes.

> **Q: When would approximate not be acceptable?**
>
> When the number is contractual or financial — an SLA with penalties, or regulatory
> reporting. Then compute exactly on a bounded window, where "bounded" is what makes it
> affordable. The general shape: approximate for monitoring, exact for money.

---

## 4. Why p95, not an average

An average hides the tail. 99 trades at 10 ms and one at 30 seconds averages to a
comfortable 310 ms, and you never learn that a request took half a minute.

p95 shows the bad end while ignoring a single freak outlier. It's also what users actually
experience: the average user has a fine time, and the ones who churn are in the tail.

We publish p50 *and* p95 because the pair carries information neither has alone. In our
data BTCUSDT ran 709 ms p50 against 1433 ms p95 — a 2× spread that says the delay isn't a
constant, it's a distribution with structure. That structure is the 1-second batch window.

> **Q: Why not p99?**
>
> At 569 rows, p99 is roughly the 6th-worst value — nearly a max, and it moves wildly run
> to run. Percentile choice has to respect sample size. p99 becomes meaningful with orders
> of magnitude more rows.

---

## 5. Aggregate late

The MV computes over raw rows. It would have been cheaper to aggregate at load time — an
Aggregate-model table, or rolling counters in the ingester.

That would have been a mistake, and it's worth being able to say why:

- **Aggregation is irreversible.** Once you store "569 events in this minute" you can never
  recover which `trade_id`s were missing. The headline metric would be impossible.
- **New questions need old data.** Every metric added later — buy pressure, the skew
  correction — was computed over rows captured before those metrics existed.
- **Late data just works.** A trade arriving after its window is included on the next
  refresh, because the whole window is recomputed. Incremental counters would need
  explicit late-arrival handling.

The cost is storage and compute. That's the right thing to spend, because storage is cheap
and lost information is not recoverable at any price.

---

## 6. Correcting clock skew in SQL

The correction from phase 1, implemented:

```sql
min(recv_ms - event_ms)                                               AS skew_est_ms,
percentile_approx(recv_ms - event_ms, 0.50)                           AS ingest_lag_raw_p50_ms,
percentile_approx(recv_ms - event_ms, 0.50) - min(recv_ms - event_ms) AS ingest_lag_p50_ms,
```

Three details that make it defensible:

- **Both numbers are published.** Showing only the corrected value would hide that a
  correction was needed — and a reviewer can't check work they can't see.
- **Scoped per window**, so slow drift is tracked rather than assumed constant.
- **Correction is non-negative by construction**, since we subtract the minimum. There's a
  test asserting exactly that.

> **Q: How do you know the correction is right rather than just flattering?**
>
> Independent agreement. The correction produced 7–12 ms p50, and separate measurement on
> the laptop gave ~8 ms. Two paths to the same answer. It's also the right *shape* — public-
> internet WebSocket latency of single-digit-to-tens of milliseconds is plausible, while the
> uncorrected −39 ms is physically impossible. A correction that turns an impossible number
> into a plausible one, and agrees with an independent measurement, has earned some trust.

---

## 7. Weighting a rate correctly

`loss_rate` is `total_missing / total_expected`, not the mean of the per-minute rates.

Those differ, and the difference is a classic error. A minute with 4 events and a minute
with 4000 are not equally informative; averaging their rates treats them as if they were,
letting a tiny quiet window dominate a busy one.

There's a unit test pinning this to `3/5003` rather than the mean of 0% and 0.3%.

> **Q: Where else does this bite?**
>
> Anywhere a ratio gets averaged: conversion rates by day, error rates by service, price
> per unit. The rule is to sum the numerators and denominators separately, then divide —
> never average ratios unless the groups really are equal-sized.

---

## 8. A note on `buy_pressure`

It came out at 0.999 — almost all volume buyer-initiated. Checked against fresh live data,
which showed the same one-sided skew (2.6%–33% maker-side across symbols), so it's a real
market condition, not a parsing bug.

Worth mentioning because "I saw a suspicious number and verified it against an independent
sample before trusting it" is exactly the instinct interviewers are probing for. It also
means the dashboard gauge will often sit near an extreme rather than mid-range — a design
constraint to handle honestly rather than by rescaling until it looks nice.

---

## The one-liner

> "The view publishes latency both raw and skew-corrected. Raw p50 is minus 39
> milliseconds, which is impossible, so the correction isn't cosmetic — and the corrected
> 7 milliseconds matches what I measured independently on a different machine."
