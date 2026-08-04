-- The pipeline's health rollup: one row per minute per symbol.
--
-- Async, so StarRocks refreshes it on its own schedule. That is the whole orchestration
-- story for this project -- no Airflow, no cron, no DAG. The warehouse maintains the
-- aggregate because the warehouse is the thing that knows when the data changed.
--
-- Grouped by recv_ms (our clock), not event_ts (the exchange's), because this view
-- measures the pipeline rather than the market. A late trade should make the minute we
-- received it look bad, not retroactively worsen a minute that already closed.

CREATE MATERIALIZED VIEW IF NOT EXISTS tapewatch.pipeline_health_1m
REFRESH ASYNC EVERY (INTERVAL 1 MINUTE)
AS
SELECT
    date_trunc('minute', from_unixtime(recv_ms DIV 1000)) AS minute,
    symbol,
    count(*)                                              AS events,

    -- Clock skew, measured rather than assumed.
    --
    -- recv_ms and event_ms come from different machines, so their difference is
    -- (true latency + skew) and one endpoint cannot separate the terms. Measured
    -- locally at -46 ms: 81% of raw values were negative, i.e. trades apparently
    -- arriving before they happened.
    --
    -- The minimum filter: within a window the smallest value belongs to the fastest
    -- trade, whose true latency is near zero, so that minimum estimates the skew.
    -- Both raw and corrected are published -- hiding the raw number would hide the
    -- fact that a correction was needed at all.
    min(recv_ms - event_ms)                               AS skew_est_ms,
    percentile_approx(recv_ms - event_ms, 0.50)           AS ingest_lag_raw_p50_ms,
    percentile_approx(recv_ms - event_ms, 0.50) - min(recv_ms - event_ms) AS ingest_lag_p50_ms,
    percentile_approx(recv_ms - event_ms, 0.95) - min(recv_ms - event_ms) AS ingest_lag_p95_ms,

    -- One clock read twice. No correction possible or needed -- the honest metric.
    percentile_approx(send_ms - recv_ms, 0.50)            AS buffer_lag_p50_ms,
    percentile_approx(send_ms - recv_ms, 0.95)            AS buffer_lag_p95_ms,

    -- Provable event loss. Binance trade_ids are monotonic per symbol, so the range
    -- says how many trades should be here and the count says how many are.
    -- A plain aggregate, not a window function: cheaper, and immune to out-of-order
    -- arrival since min/max don't care what order rows land in.
    --
    -- ponytail: scoped to the minute window, so a trade landing either side of a
    -- boundary reads as one missing event. Move to a window function spanning
    -- windows if that false-positive rate ever matters.
    max(trade_id) - min(trade_id) + 1 - count(DISTINCT trade_id) AS missing_events,

    -- The one business metric, so a non-technical viewer has something to hold on to.
    sum(CASE WHEN is_buyer_maker = false THEN qty ELSE 0 END) / nullif(sum(qty), 0) AS buy_pressure,

    max(load_ts)                                          AS last_load_ts
FROM tapewatch.trades
GROUP BY 1, 2;
