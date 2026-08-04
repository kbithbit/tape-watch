# Phase 2 — Warehouse schema and loading

What we built: [sql/schema.sql](../../sql/schema.sql) (two tables),
`StreamLoadSink` in [ingester.py](../../ingester.py), [sr.py](../../sr.py) (thin MySQL-protocol
helpers), and [.github/workflows/test.yml](../../.github/workflows/test.yml) — CI that boots a real
StarRocks and proves the gap metric.

The fixture: a real 1-minute capture with **three ETHUSDT trades removed from the middle**
of the sequence and BTCUSDT left intact. Expected answer — ETHUSDT 3 missing, BTCUSDT 0.

---

## Topics covered

1. StarRocks table models, and why Duplicate Key here
2. Partitioning vs bucketing
3. Sort keys and the prefix index
4. Stream Load vs Routine Load vs Broker Load
5. Load labels and idempotency
6. Replication, and what `replication_num = 1` costs
7. Testing a pipeline against a known answer

---

## 1. Table models

StarRocks has four, and picking one is a real design decision:

| Model | Behaviour | Use for |
|---|---|---|
| **Duplicate Key** | Keeps every row. No dedup, no merge. | Append-only logs, events, facts |
| **Aggregate** | Pre-aggregates on load by key (SUM/MAX/REPLACE) | Rollups where raw rows are never needed |
| **Unique Key** | Latest row per key wins, merged on read | Upserts where read cost is acceptable |
| **Primary Key** | Real-time upsert and delete, merged on write | CDC from an OLTP database |

We use **Duplicate Key**, and there are three reasons worth being able to give:

1. A trade is an immutable fact. Binance never revises trade 4242. There is nothing to update.
2. It's the cheapest write path — no key lookup, no merge, no in-memory index.
3. **It protects the headline metric.** A Primary Key table would silently collapse rows
   sharing a key, so duplicates would vanish rather than being counted. When the whole
   project measures event loss, a table model that quietly hides row-level anomalies
   undermines the thing being measured.

Point 3 is the one interviewers remember, because it shows the schema choice was driven
by what the data has to prove, not by habit.

> **Q: When would you switch to Primary Key?**
>
> When the source revises rows — CDC from an OLTP database, where you want the current
> state of each record, not its history. Also when you need `DELETE` for GDPR-style
> erasure, which Duplicate Key can only do by rewriting a partition.

> **Q: Why not Aggregate, since you only ever query the rollup?**
>
> Because the rollup isn't the only question. Aggregate tables discard raw rows at load
> time, and the gap metric needs individual `trade_id`s — you cannot recover which IDs
> were missing from a table that only kept counts. Aggregating early is irreversible;
> keeping raw rows and computing views over them stays reversible. That's a general
> principle: **aggregate late.**

---

## 2. Partitioning vs bucketing

Two different splits, constantly confused, and being crisp about the difference is a
reliable way to sound like you've actually run a warehouse.

- **Partitioning** — a *logical* split by column value. Ours is `date_trunc('day', event_ts)`.
  Buys **partition pruning** (a query for today reads one day's files) and cheap retention
  (dropping a month is a metadata operation, not a delete).
- **Bucketing** — a *physical* split inside each partition, by hash. Ours is
  `HASH(symbol) BUCKETS 4`. Each bucket is a **tablet**: the unit of parallelism, replication
  and data movement.

Roughly: partitions decide *what gets read*, buckets decide *how many workers read it*.

We use **expression partitioning**, so partitions are created automatically on write.
The older approach needed pre-created ranges or a dynamic-partition config — one more
thing to forget until a load fails at midnight.

> **Q: Is your bucketing choice good?**
>
> No, and it's commented in the schema. Hashing on `symbol` with three distinct values
> means at most three non-empty tablets out of four — one bucket is dead, and write
> parallelism is capped at three regardless of `BUCKETS`. Classic low-cardinality skew.
>
> It's deliberate at this scale: volume is tiny and it colocates each symbol's rows, so
> per-symbol queries touch one tablet. At real volume I'd hash on `trade_id` for an even
> spread, accepting that symbol queries then fan out across all tablets.
>
> The general rule: bucket on something high-cardinality and evenly distributed, and size
> tablets around a gigabyte. Too few and you lose parallelism; too many and metadata
> overhead dominates.

---

## 3. Sort keys and the prefix index

`DUPLICATE KEY (event_ts, symbol, trade_id)` does double duty: it names the model *and*
defines the **sort key**. Rows are stored sorted by those columns, so StarRocks builds a
sparse **prefix index** over roughly the first 36 bytes of that key — the mechanism that
turns a filter into a skip rather than a scan.

Two consequences that catch people out:

- Sort key columns must be the **first** columns in the table definition, in order. You
  cannot sort by a column declared late.
- The prefix index **stops at a VARCHAR column**. Ours effectively covers `event_ts` and
  `symbol`; `trade_id` sits outside it. That's fine — filtering by time and symbol is the
  common access pattern — but it means a query filtering only on `trade_id` gets no index
  help and scans.

> **Q: Why is column order in a sort key a bigger deal than in a normal index?**
>
> Because there's only one. A row-store can carry a dozen secondary indexes; a columnar
> table has a single physical ordering, so the sort key is a one-shot decision that
> everything else must live with. Put the column you filter on most first.

---

## 4. Stream Load vs Routine Load vs Broker Load

| Method | Shape | Use for |
|---|---|---|
| **Stream Load** | Synchronous HTTP PUT, you push, it returns the result | Micro-batches from an app — ours |
| **Routine Load** | StarRocks continuously pulls from Kafka | Long-running ingestion from a broker |
| **Broker Load** | Asynchronous, StarRocks pulls large files | Bulk from S3/HDFS |

Stream Load fits because **we already hold the data** — the ingester has the rows in memory
and wants to know immediately whether the write succeeded. Routine Load would be right if
Kafka existed, and it's the natural upgrade if durability is ever added.

Each Stream Load request is **one transaction**: all rows commit or none do. That's why
batch size is a correctness boundary, not just a performance knob.

> **Q: Why did the redirect need handling by hand?**
>
> The FE answers Stream Load with a 307 pointing at the backend that should take the
> write. `requests` follows redirects but **strips the Authorization header across hosts**
> — a sensible security default that here produces a puzzling 401 on the second hop. So
> we disable automatic redirects and re-send explicitly with auth reattached.
>
> There's a unit test for exactly this, because it's the kind of bug that only appears
> against a real cluster and is miserable to debug in CI.

---

## 5. Labels and idempotency

Every Stream Load carries a **label**. StarRocks refuses a label it has already committed,
which makes labels the deduplication mechanism.

Ours is `"tw-" + sha1(payload)` — **content-addressed**. Same rows produce the same label,
so a retry after an ambiguous timeout cannot double-load. No state to track, no counter to
persist, no coordination.

This is the answer to the hardest failure in any loader: *the request timed out — did it
commit or not?* With a content-addressed label you simply retry. If it committed, StarRocks
says "Label Already Exists" and you treat that as success, which the code does explicitly.

> **Q: Does that give you exactly-once?**
>
> Exactly-once *into the warehouse*, for any batch that reaches it — which is the part
> we control. It does not make the pipeline exactly-once end to end, because events lost
> during a WebSocket disconnect never reach the loader at all. Labels solve duplication,
> not loss. Being precise about which half of the problem a mechanism solves matters more
> than claiming the buzzword.

> **Q: Why hash the content instead of using a UUID or a counter?**
>
> A UUID differs on every retry, so retries duplicate — it defeats the purpose. A counter
> needs durable state, which is a new failure mode. The content *is* the identity, so
> hashing it needs nothing else.

---

## 6. `replication_num = 1`

One copy of every tablet. Lose the backend, lose the data.

Correct here — CI throws the cluster away every run, so paying 3× storage and 3× write
cost for redundancy that outlives the data by zero seconds would be silly. In production
the default of 3 exists so a node failure costs nothing.

> **Q: What else does replication buy besides durability?**
>
> Read parallelism and rolling upgrades. With three replicas the optimiser can read from
> whichever is least loaded, and you can restart one backend without the table going
> unavailable. Durability is the headline, availability is the daily benefit.

---

## 7. Testing against a known answer

The single most important idea in this phase.

**You cannot test a data pipeline against live data**, because you don't know the right
answer. If the query says 4 trades are missing, is the pipeline lossy, or did the market
do that?

So the fixture is a real capture that was then **deliberately damaged**: three ETHUSDT
trades removed from the middle, BTCUSDT untouched. Now there is exactly one correct answer
— 3 and 0 — and one assertion covers the schema, Stream Load, the redirect handling, the
column mapping, and the metric together.

Removing them from the *middle* matters: the metric is `max - min + 1 - count`, so deleting
the first or last trade shrinks the range too and hides the gap. The test would pass while
the metric silently under-reported.

There is a second assertion for idempotency: replay the same fixture and confirm the row
count doesn't move.

> **Q: Why not mock StarRocks in the unit tests?**
>
> The unit tests do mock the HTTP layer — label determinism, redirect handling and failure
> counting all run offline in milliseconds. But a mock only ever proves the code matches
> your *belief* about StarRocks. Every real bug here — the redirect stripping auth, the
> DATETIME precision question, expression partitioning syntax — lives precisely in the gap
> between that belief and reality. So: mock for logic, real cluster for the contract.

---

## 8. What actually broke on first contact

Worth rehearsing, because "tell me about a bug you hit" is a near-certain question and a
specific answer beats a generic one. Three failures, none of which local testing could
have caught:

**1. `pytest` failed in CI, passed locally.** Locally I ran `python -m pytest`, which puts
the working directory on `sys.path`; CI ran bare `pytest`, which doesn't. Fixed with
`pythonpath = .` in `pytest.ini`. The lesson is that *how* you invoke a test runner is part
of the test environment.

**2. The fixture predated the schema.** `gaps.jsonl` was captured before `event_ts` was
added to the row shape, so every row failed the non-nullable partition column. Fixtures are
code, and they drift from the schema exactly like code does.

**3. A column DEFAULT is not applied to a JSON load that omits the key.** This is the real
StarRocks lesson. `load_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP` looks like it should
fill itself in. It doesn't — Stream Load rejected every row with *"NULL value in non-nullable
column 'load_ts'"*. The default applies to `INSERT`, not to a load that never mentions the
column. The fix is naming it explicitly as a derived column in the mapping:
`columns: ..., load_ts=current_timestamp()`.

All three surfaced within four CI runs, because the diagnostics were fixed before the bug
was. The first failure said only *"too many filtered rows"* — useless. StarRocks returns an
`ErrorURL` holding the per-row cause, so the loader now follows it and logs the real reason.
The next run named the exact column.

> **Q: What would you do differently?**
>
> Follow `ErrorURL` from the start. I spent one CI cycle on a generic message when the
> specific one was one HTTP request away. Generally: when a system gives you a summary
> error and a pointer to detail, wire up the pointer before you start guessing.

---

## The one-liner

> "The test loads a real capture with three trades surgically removed, and asserts the
> pipeline finds exactly those three — and zero on the untouched symbol. Live data can't
> tell you the right answer; a doctored fixture can."
