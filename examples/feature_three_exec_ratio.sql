CREATE TABLE hl_l2book (
  coin STRING,
  event_time TIMESTAMP_LTZ(3),
  side STRING,         
  level_idx INT,
  px DECIMAL(38,18),
  sz DECIMAL(38,18),
  n BIGINT,
  WATERMARK FOR event_time AS event_time - INTERVAL '2' SECOND
) WITH (
  'connector' = 'ws',
  'ws.url' = 'wss://api.hyperliquid.xyz/ws',
  'ws.source' = 'hyperliquid',
  'ws.channel' = 'l2Book',
  'ws.symbol' = 'ETH',
  'ws.queue.capacity' = '200',
  'ws.request.batch' = '50',
  'ws.connect.timeout.ms' = '10000'
);

CREATE TABLE hl_trades (
  coin STRING,
  event_time TIMESTAMP_LTZ(3),
  side STRING,          -- 'B' or 'S'
  px DECIMAL(38,18),
  sz DECIMAL(38,18)
) WITH (
  'connector' = 'ws',
  'ws.url' = 'wss://api.hyperliquid.xyz/ws',
  'ws.source' = 'hyperliquid',
  'ws.channel' = 'trades',
  'ws.symbol' = 'ETH',
  'ws.queue.capacity' = '200',
  'ws.request.batch' = '50',
  'ws.connect.timeout.ms' = '10000'
);

CREATE TABLE print_sink_exec_ratio (
  coin STRING,
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  side STRING,
  depth_k_max DECIMAL(38,18),
  vanish_ratio DECIMAL(38,18),
  trade_vol DECIMAL(38,18),
  exec_ratio DECIMAL(38,18)
) WITH (
  'connector' = 'print'
);

CREATE VIEW book_k AS (
  SELECT
    coin,
    window_start,
    window_end,
    side,                 -- 'BID'/'ASK'
    SUM(sz) AS depth_k
  FROM TABLE(
    TUMBLE(TABLE hl_l2book, DESCRIPTOR(event_time), INTERVAL '1' SECOND)
  )
  WHERE level_idx <= 5
  GROUP BY coin, window_start, window_end, side
;

CREATE VIEW book_5s AS (
  SELECT
    coin,
    window_start,
    window_end,
    side,
    MAX(depth_k) AS depth_k_max,
    MIN(depth_k) AS depth_k_min
  FROM TABLE(
    TUMBLE(TABLE book_k, DESCRIPTOR(window_start), INTERVAL '5' SECOND)
  )
  GROUP BY coin, window_start, window_end, side
;

CREATE VIEW trade_5s AS (
  SELECT
    coin,
    window_start,
    window_end,
    side,                 -- 'B'/'S'
    SUM(sz) AS trade_vol
  FROM TABLE(
    TUMBLE(TABLE hl_trades, DESCRIPTOR(event_time), INTERVAL '5' SECOND)
  )
  GROUP BY coin, window_start, window_end, side
;

CREATE VIEW book_trade AS (
  SELECT
    b.coin,
    b.window_start,
    b.window_end,
    b.side AS book_side,
    b.depth_k_max,
    b.depth_k_min,
    (b.depth_k_min / NULLIF(b.depth_k_max, 0)) AS vanish_ratio,
    t.trade_vol,
    CASE
      WHEN b.side = 'BID' THEN 'B'
      ELSE 'S'
    END AS expected_trade_side
  FROM book_5s b
  LEFT JOIN trade_5s t
    ON b.coin = t.coin
   AND b.window_start = t.window_start
   AND b.window_end = t.window_end
   AND t.side = CASE WHEN b.side='BID' THEN 'B' ELSE 'S' END
);

INSERT INTO print_sink_exec_ratio
SELECT
  coin,
  window_start,
  window_end,
  book_side,
  depth_k_max,
  vanish_ratio,
  COALESCE(trade_vol, CAST(0 AS DECIMAL(38,18))) AS trade_vol,
  -- 关键：成交跟随不足比率
  COALESCE(trade_vol, CAST(0 AS DECIMAL(38,18))) / NULLIF(depth_k_max, 0) AS exec_ratio
FROM book_trade;