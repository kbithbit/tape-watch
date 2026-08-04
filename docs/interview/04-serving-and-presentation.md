# Phase 4 — Serving and presentation

What we built: [dashboard.html](../../dashboard.html) — one file, no build step, no chart
library, no dependencies. Plus a real bug found by looking at the rendered page.

---

## Topics covered

1. Why the dashboard reads a file, not the warehouse
2. The timezone bug
3. Freshness: what "fresh" actually means
4. Labelling metrics honestly
5. Accessibility as correctness
6. Why no chart library

---

## 1. The serving layer reads pre-computed output

The dashboard never queries StarRocks. It fetches two static JSON files.

That's not laziness, it's the standard shape of a serving layer, and there are four
reasons worth being able to give:

- **The warehouse isn't there.** StarRocks exists only during a CI run. A dashboard that
  queried it directly would be broken 99% of the time.
- **Query cost per viewer.** Reading a file costs nothing per visitor; a query costs
  compute every time someone refreshes.
- **Blast radius.** A misbehaving dashboard cannot take down the warehouse if it has no
  connection to it.
- **Credentials.** A browser querying the warehouse needs credentials in the browser.
  There is no safe way to do that.

This is the same reasoning behind serving layers generally — a cache, a read replica, a
materialised export. The pattern is: *analytical systems are not designed to be queried
by end users at end-user concurrency.*

> **Q: What's the cost of this design?**
>
> Staleness and no interactivity. Nobody can slice by a dimension you didn't pre-compute,
> and the numbers are as old as the last run. For an ad-hoc analytics tool that would be
> disqualifying; for a status dashboard answering fixed questions, it's the right trade.

---

## 2. The timezone bug — the best story in this phase

The dashboard rendered **"Data freshness: −28796s."** Data arriving 8 hours in the future.

−28796 seconds is −7.999 hours. **An exact whole-hour offset in a time delta is almost
never a clock problem — it's a timezone problem.** That single heuristic pointed straight
at the cause.

The cause: **StarRocks Stream Load defaults its `timezone` parameter to `Asia/Shanghai`.**
So `load_ts = current_timestamp()` was evaluated in UTC+8, while the query session read
`now()` as UTC. Every row landed with a timestamp 8 hours ahead.

The fix is two lines, one on each side of the boundary:

```python
headers["timezone"] = "UTC"                      # the write
init_command = "SET time_zone = '+00:00'"        # the read
```

Three things worth saying about it:

- **It was invisible in the data.** Every row loaded, every test passed, the gap metric was
  correct. Only a *derived* metric — a difference between two timestamps written by
  different components — exposed it. Bugs at system boundaries hide inside individual
  components that are each internally consistent.
- **It was caught by rendering the page**, not by a test. The number was in `metrics.json`
  the whole time; nobody had looked at it. Looking at your output is a debugging technique.
- **The general rule: never inherit a timezone default.** Any system with a configurable
  timezone will have a default you didn't choose, and it will differ between the component
  that writes and the component that reads. State it explicitly on both sides.

> **Q: How did you make sure it stays fixed?**
>
> An e2e test asserting `staleness_s >= 0`, written *before* the fix so it failed first.
> It's a cheap invariant with real teeth — data cannot arrive in the future, so any future
> timezone regression trips it immediately.

---

## 3. Freshness

`staleness_s = now() - max(load_ts)` — seconds since anything last landed. It's the single
most useful health metric, because it's the one that fires when the pipeline is *silent*.

Silence is the failure mode monitoring usually misses. Error-rate alerts need errors; a
pipeline that stopped producing has no errors at all. Freshness is what catches "nothing is
wrong, nothing is happening."

Two thresholds on the dashboard: 120 s → degraded, 600 s → critical. Both arbitrary, and
that's honest — a threshold with no SLA behind it is a guess. In a real system it would
come from what downstream consumers can tolerate.

> **Q: Freshness of what — the data or the pipeline?**
>
> Two different questions, and the dashboard shows both. `staleness_s` measures the
> pipeline *within* a run. "Last run 30 min ago" measures whether the pipeline is running
> at all. A pipeline can be perfectly fresh inside a run that last happened yesterday —
> which is why the run age is its own panel with a `history` scope chip.

---

## 4. Labelling honestly

The design constraint that shaped this page: **never imply coverage the pipeline doesn't
have.**

- **Every panel carries a scope chip** — `this run` or `history`. StarRocks is destroyed
  between runs, so "last 24 hours" would be a lie. The chip makes the boundary visible
  instead of hiding it.
- **Raw and corrected latency are both published.** Showing only the skew-corrected number
  would conceal that a correction was applied at all.
- **Buy pressure is per symbol, never averaged.** Averaging two symbols' ratios would
  weight a quiet symbol equally with a busy one — the same error the exporter avoids for
  loss rate. When you can't weight correctly, don't aggregate.
- **The sample-data banner.** Open the page without `metrics.json` and it renders an
  embedded fixture *behind a visible warning*, rather than silently showing plausible
  numbers.

> **Q: Isn't a scope chip on every panel visual clutter?**
>
> It's eleven characters against the risk of someone reading a 3-minute sample as
> continuous monitoring. A dashboard's job is to be correctly understood, not merely to
> look clean. If a caveat matters, it belongs next to the number — not in a footnote
> nobody reads.

---

## 5. Accessibility as correctness

Not decoration — these are the same discipline as the rest:

- **Colour never carries meaning alone.** Status chips pair the colour with a glyph *and* a
  word ("Healthy", "Degraded"). Roughly 1 in 12 men has some colour vision deficiency; a
  red/green dot alone is unreadable to them.
- **The palette was validated, not eyeballed.** The two series colours were run through a
  CVD simulator: worst-pair ΔE 24.7 in light mode, 26.8 in dark, against a target of ≥8.
- **A table view exposes every value.** Charts are a summary; the table is the ground truth,
  and it works with a screen reader.
- **Both themes are designed, not flipped.** The dark palette is separately chosen steps
  validated against the dark surface — an automatic inversion produces colours that fail
  contrast.

---

## 6. Why no chart library

Chart.js, D3, Recharts — all would have worked. The whole page is ~200 lines of vanilla JS
and inline SVG instead.

The reasoning: this needs two line charts, a bar chart and a meter. A library brings
hundreds of kilobytes, a build step, a dependency to keep patched, and a default visual
style to fight. SVG paths are not hard — `M x,y L x,y` — and there's nothing to break in
two years when the library has a major version bump.

> **Q: When would you add one?**
>
> Zoom, brushing, dense scatter with hit-testing, or a dozen chart types across a real app —
> anywhere I'd be reimplementing a library badly. For four static charts, the dependency
> costs more than it saves.

---

## The one-liner

> "The dashboard showed data arriving 8 hours in the future. An exact whole-hour offset in
> a time delta is a timezone, not a clock — StarRocks Stream Load defaults to
> Asia/Shanghai while my query session read UTC. Two lines to fix, and an e2e test
> asserting freshness can never be negative so it can't come back."
