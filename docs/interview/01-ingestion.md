# Phase 1 — Ingestion

What we built: [ingester.py](../../ingester.py) — holds a WebSocket to Binance, stamps
clocks on every trade, batches them, and hands each batch to a sink.

Measured on a real 20-second capture (1308 trades, 0 malformed):

| Metric | min | p50 | p95 | max |
|---|---|---|---|---|
| `recv_ms - event_ms` (cross-clock) | −46 | −38 | 51 | 57 |
| `send_ms - recv_ms` (our clock) | 0 | 506 | 1434 | 1842 |

Keep those numbers. Interviewers respond very differently to "about 500 ms" than to
"p50 506 ms, p95 1434 ms, and here's why the p95 is nearly triple the p50."

---

## Topics covered

1. WebSocket vs REST polling
2. Event time vs processing time
3. Clock skew and why one-way latency is not directly measurable
4. Batching: the latency/throughput trade-off
5. Backpressure
6. Delivery semantics (at-least-once, at-most-once, exactly-once)
7. Failure handling: reconnect with backoff

---

## 1. WebSocket vs REST polling

**Polling** = you ask "anything new?" on a timer. **WebSocket** = one connection stays
open and the server pushes data the moment it exists.

For trades, polling is wrong on both ends. Poll every second and you miss the ordering
and timing of everything inside that second — Binance produced ~65 trades/sec across
three symbols in our sample. Poll faster and you burn rate limit asking a question whose
answer is usually "nothing."

The deeper point: **polling makes your data's timeliness a function of your poll
interval, not of reality.** With a push stream, the data's timing is the market's timing.

> **Q: When would you still choose polling?**
>
> When the source has no push option (most REST APIs), when updates are rare enough that
> an idle connection isn't worth holding, or when you genuinely only need periodic
> snapshots rather than every change — a daily currency rate doesn't need a socket.
> Polling is also far easier to operate: no reconnect logic, no partial-message handling,
> and it recovers from failure by simply running again.

---

## 2. Event time vs processing time

- **Event time** — when the thing happened in the real world. Binance's `T` field.
- **Processing time** — when your system saw it. Our `recv_ms`.

They are never equal, and the gap is not constant.

This matters because analysis must use event time. If a trade happens at 14:59:59.9 but
reaches you at 15:00:00.1, processing time files it in the wrong minute. Do that across a
day and your hourly numbers are quietly wrong at every boundary.

But observability must use processing time — "how much did we ingest in the last minute"
is a question about *our* minute, not the market's.

> **Q: Which does your materialized view group by, and why?**
>
> Processing time (`recv_ms`), because the view measures the pipeline, not the market.
> Grouping health metrics by event time would be self-defeating: a late-arriving trade
> would land in an old window and make a past minute look retroactively worse, while the
> minute where the lateness actually hurt looks fine.
>
> A business-facing view over the same table would group by event time instead. Same raw
> rows, two views, two different questions. That separation is the point of keeping the
> raw table unaggregated.

> **Q: What's a watermark?**
>
> A streaming system's assertion that it does not expect any more events older than time
> T, so windows up to T can be closed and emitted. It's a bet: too aggressive and you drop
> late data, too conservative and every result is delayed.
>
> This project has no watermarks, deliberately. Windows are computed by re-aggregating the
> raw table, so a late arrival is simply included next refresh. That's the luxury of
> keeping raw rows and recomputing rather than maintaining incremental state.

---

## 3. Clock skew — the one that impresses

This is the strongest thing in this phase, because it's a real defect found in real data.

`recv_ms - event_ms` was **negative for 81% of trades**. Trades appeared to arrive before
they happened. Nothing was broken: the local clock ran about 46 ms behind Binance's.

The general principle: **you cannot measure one-way latency between two clocks you don't
control.** What you measure is `true_latency + skew`, and one endpoint cannot separate the
two terms. (Round-trip time avoids this by using one clock twice, but a push stream gives
you no round trip.)

The fix is a **minimum filter**: over a window, the smallest observed value belongs to the
fastest trade, whose true latency is near zero — so that minimum estimates the skew.
Subtract it for a usable *relative* latency. Applying it here: min was −46, so corrected
p50 ≈ 8 ms, p95 ≈ 97 ms. Those are believable numbers for the public internet, which is
itself a sanity check that the correction is sound.

This is also why the design has three clocks rather than two. `send_ms - recv_ms` reads
one clock twice, so it's exact and needs no correction. Knowing which of your metrics are
trustworthy and which are estimates is the actual skill.

> **Q: Why not just run NTP and assume the clocks agree?**
>
> NTP typically gets you within a few milliseconds, which is fine when your latencies are
> hundreds of milliseconds — but ours are tens, so a few ms of skew is a meaningful
> fraction. More importantly, you control your clock and not the exchange's. Assuming
> agreement means silently trusting a number you never verified. The minimum filter costs
> one aggregate and removes the assumption.

> **Q: When does the minimum filter fail?**
>
> When no event in the window has near-zero latency — if the network is uniformly
> congested, the minimum is an overestimate of skew and you understate latency. It also
> can't detect *drift*, where skew changes over time; a per-window minimum handles slow
> drift, but a clock jump mid-window corrupts that window.

---

## 4. Batching

`send_ms - recv_ms` had p50 506 ms against a 1-second batch window — exactly right. Rows
arrive uniformly through the window, so the average row waits half of it.

**That means our own batching, not the network, is the dominant latency term.** Corrected
network latency was ~8 ms p50; batching adds ~506 ms. Being able to say "the slowest part
of my pipeline is a choice I made, and here's the knob" is a strong interview position.

The trade-off:

| Smaller batches | Larger batches |
|---|---|
| Lower latency | Higher throughput |
| More requests, more overhead per row | Fewer, larger writes |
| More small files in the warehouse | Better compression and scan efficiency |

Small writes are especially costly in a columnar warehouse: each load creates a data
version that background compaction must later merge. StarRocks explicitly recommends
batching over frequent tiny loads for this reason.

We flush on **whichever trips first — 500 rows or 1 second.** Size alone stalls forever on
a quiet market; age alone wastes a request per trade during a busy one. Needing both
conditions is the general pattern.

> **Q: p95 was 1434 ms with a 1-second window. Why does it exceed the window?**
>
> The flush check runs after a message arrives, and the socket read has its own 1-second
> timeout. A row can arrive just before a quiet stretch and wait out both. It's bounded at
> roughly 2× the window, which is acceptable here — but if that mattered, the fix is a
> separate timer task flushing independently of message arrival, rather than checking the
> deadline only on the receive path.

---

## 5. Backpressure

Backpressure is what a system does when data arrives faster than it can be handled. The
four options: **buffer** (memory grows until it doesn't), **drop** (lose data, hopefully
on purpose), **block** (stop reading, push the problem upstream), or **scale**.

We buffer, unbounded, and that is a real limitation. If StarRocks stalled, the batch list
would grow until the process died.

> **Q: What would you add?**
>
> A bounded buffer with an explicit policy on overflow — for this project, drop and
> *count* the drops, since the whole premise is that loss is measured rather than
> prevented. A silent unbounded queue is the worst option: it converts a visible
> throughput problem into an invisible memory leak that fails hours later, far from
> the cause.
>
> Blocking isn't available to us in any useful sense: stop reading the socket and Binance
> disconnects us, so the data is lost anyway — just less visibly.

---

## 6. Delivery semantics

- **At-most-once** — never duplicated, may be lost.
- **At-least-once** — never lost, may be duplicated.
- **Exactly-once** — every event lands once. Expensive, and usually means at-least-once
  delivery plus deduplication at the sink.

Right now this is **at-most-once**: a batch that fails is dropped. Phase 2's Stream Load
labels move it toward at-least-once, because a retried batch reusing its label is rejected
as a duplicate rather than double-loaded.

> **Q: Isn't at-most-once a bad choice?**
>
> It's a deliberate one. The premise of the project is measuring loss, not preventing it —
> and a WebSocket that drops gives you no replay, so events lost during a disconnect are
> unrecoverable no matter what the sink does. Retry logic would create an illusion of
> completeness the source cannot support.
>
> Real durability means a replayable buffer between source and sink — Kafka, or writing
> raw frames to disk first. That's the honest upgrade path, and it's a genuine cost, not
> a line of code.

---

## 7. Reconnect and backoff

Any failure reconnects with exponential backoff (doubling, capped at 30 s) and **full
jitter** — sleeping a random duration in `[0, backoff)` rather than the full value.

> **Q: Why randomise the backoff?**
>
> Without jitter, everything that failed together retries together, and the retry storm
> re-breaks whatever just recovered. Randomising spreads the load. It matters less for one
> client than for a fleet, but it's free and it's the correct habit.

Every connect and disconnect is recorded for the `ingest_events` table, so the dashboard
can show reconnect counts. **A pipeline that hides its own failures isn't observable** —
if the ingester reconnected forty times overnight, that has to be visible somewhere, or
you'll conclude the data is fine when it has holes in it.

---

## The one-liner

> "Three clocks per row. Two are ours, so the difference between them is exact; one is the
> exchange's, so that difference includes clock skew — which I measured at 46 ms and
> corrected with a minimum filter. The largest latency term turned out to be my own
> 1-second batching window, not the network."
