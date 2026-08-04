-- tape-watch schema. Two tables: the trade log, and the pipeline's own failure log.

CREATE DATABASE IF NOT EXISTS tapewatch;

-- Raw landing table. Append-only, never updated, never deduplicated.
--
-- DUPLICATE KEY (not PRIMARY KEY) is the point: a trade is an immutable fact that the
-- exchange never revises, so there is nothing to merge, and the write path stays as
-- cheap as possible. It also protects the headline metric -- a Primary Key table would
-- silently collapse rows sharing a key, and duplicate trade_ids would disappear instead
-- of being counted.
--
-- The three clocks are BIGINT epoch milliseconds rather than timestamps so the lag
-- arithmetic is exact integer subtraction, independent of how the server's DATETIME
-- type handles fractional seconds.
CREATE TABLE IF NOT EXISTS tapewatch.trades (
    event_ts       DATETIME       NOT NULL  COMMENT "exchange clock, second precision, partition key",
    symbol         VARCHAR(20)    NOT NULL,
    trade_id       BIGINT         NOT NULL  COMMENT "monotonic per symbol -- the basis of gap detection",
    price          DECIMAL(20, 8) NOT NULL,
    qty            DECIMAL(20, 8) NOT NULL,
    is_buyer_maker BOOLEAN        NOT NULL  COMMENT "true => seller-initiated",
    event_ms       BIGINT         NOT NULL  COMMENT "exchange clock",
    recv_ms        BIGINT         NOT NULL  COMMENT "our clock, socket read",
    send_ms        BIGINT         NOT NULL  COMMENT "our clock, batch dispatched",
    load_ts        DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT "server clock, for freshness only"
)
DUPLICATE KEY (event_ts, symbol, trade_id)
PARTITION BY date_trunc('day', event_ts)
-- ponytail: hashing on symbol with only 3 distinct values leaves buckets empty and
-- caps write parallelism at 3 tablets. Deliberate here -- it colocates each symbol's
-- rows and the volume is tiny -- but hash on trade_id if the symbol list stays short
-- and throughput starts to matter.
DISTRIBUTED BY HASH (symbol) BUCKETS 4
PROPERTIES ("replication_num" = "1");

-- Every connect, disconnect and failed load. A pipeline that hides its own failures
-- is not observable: without this, a run that dropped half its batches looks identical
-- to a quiet market.
CREATE TABLE IF NOT EXISTS tapewatch.ingest_events (
    ts     DATETIME     NOT NULL,
    kind   VARCHAR(20)  NOT NULL  COMMENT "connect | disconnect | load_error",
    detail VARCHAR(500)
)
DUPLICATE KEY (ts, kind)
DISTRIBUTED BY HASH (ts) BUCKETS 1
PROPERTIES ("replication_num" = "1");
