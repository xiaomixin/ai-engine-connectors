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

CREATE TABLE print_sink_vanish_speed (
  coin STRING,
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  side STRING,
  depth_k_max DECIMAL(38,18),
  depth_k_min DECIMAL(38,18),
  vanish_ratio DECIMAL(38,18)
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
    TUMBLE(TABLE hl_l2book, DESCRIPTOR(event_time), INTERVAL '1' SECOND)
  )
  WHERE level_idx <= 5
  GROUP BY coin, window_start, window_end, side
;

CREATE VIEW book_5s AS
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

INSERT INTO print_sink_vanish_speed
SELECT
  coin,
  window_start,
  window_end,
  side,
  depth_k_max,
  depth_k_min,
  depth_k_min / NULLIF(depth_k_max, 0) AS vanish_ratio
FROM book_5s;
