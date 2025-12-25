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

CREATE TABLE print_sink_depth_surge (
  coin STRING,
  window_start TIMESTAMP(3),
  side STRING,
  depth_k DECIMAL(38,18),
  depth_k_prev DECIMAL(38,18),
  depth_surge_abs DECIMAL(38,18),
  depth_surge_ratio DECIMAL(38,18)
) WITH (
  'connector' = 'print'
);

CREATE VIEW book_k AS 
  SELECT
    coin,
    window_start,
    window_end,
    side,
    SUM(sz) AS depth_k
  FROM TABLE(
    TUMBLE(TABLE hl_l2book, DESCRIPTOR(event_time), INTERVAL '5' SECOND)
  )
  WHERE level_idx <= 5
  GROUP BY coin, window_start, window_end, side
;

CREATE VIEW book_k_lag AS 
  SELECT
    coin,
    window_start,
    side,
    depth_k,
    LAG(depth_k, 1) OVER (PARTITION BY coin, side ORDER BY window_start) AS depth_k_prev
  FROM book_k;

INSERT INTO print_sink_depth_surge
SELECT
  coin,
  window_start,
  side,
  depth_k,
  depth_k_prev,
  (depth_k - depth_k_prev) AS depth_surge_abs,
  (depth_k - depth_k_prev) / NULLIF(depth_k_prev, 0) AS depth_surge_ratio
FROM book_k_lag;
