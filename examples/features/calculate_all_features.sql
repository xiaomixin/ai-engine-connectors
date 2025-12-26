CREATE TABLE l2_book_flat (
  coin STRING,
  side STRING,             
  level_idx INT,       
  px DECIMAL(38, 18),
  sz DECIMAL(38, 18),
  n BIGINT,
  event_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '2' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'ods_hl_l2book_flat',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'ods_hl_l2book_flat',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json'
);

CREATE TABLE trades (
  coin STRING,
  side STRING,             
  px DECIMAL(38, 18),
  sz DECIMAL(38, 18),
  event_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '2' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'ods_hl_trades_flat',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'ods_hl_trades_flat',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json'
);

CREATE VIEW pepe_depth_1s AS
SELECT
  window_start,
  window_end,
  side,
  SUM(sz) AS depth_k
FROM TABLE(
  TUMBLE(
    TABLE l2_book_flat,
    DESCRIPTOR(event_time),
    INTERVAL '1' SECOND
  )
)
WHERE coin = 'PEPE'
  AND level_idx <= 5
GROUP BY window_start, window_end, side;

CREATE VIEW pepe_feature_f1_1s AS
SELECT
  window_start,
  window_end,
  side,
  depth_k,
  LAG(depth_k, 1) OVER (PARTITION BY side ORDER BY window_start) AS depth_k_prev,
  (depth_k - LAG(depth_k, 1) OVER (PARTITION BY side ORDER BY window_start)) AS depth_surge_abs,
  (depth_k - LAG(depth_k, 1) OVER (PARTITION BY side ORDER BY window_start))
    / NULLIF(LAG(depth_k, 1) OVER (PARTITION BY side ORDER BY window_start), 0) AS depth_surge_ratio
FROM pepe_depth_1s;


CREATE VIEW pepe_feature_f2_5s AS
SELECT
  window_start,
  window_end,
  side,
  MAX(depth_k) AS depth_k_max,
  MIN(depth_k) AS depth_k_min,
  MIN(depth_k) / NULLIF(MAX(depth_k), 0) AS vanish_ratio
FROM TABLE(
  TUMBLE(
    TABLE pepe_depth_1s,
    DESCRIPTOR(window_start),
    INTERVAL '5' SECOND
  )
)
GROUP BY window_start, window_end, side;


CREATE VIEW pepe_trade_vol_5s AS
SELECT
  window_start,
  window_end,
  side,                -- 'B' or 'S'
  SUM(sz) AS trade_vol
FROM TABLE(
  TUMBLE(
    TABLE trades,
    DESCRIPTOR(event_time),
    INTERVAL '5' SECOND
  )
)
WHERE coin = 'PEPE'
GROUP BY window_start, window_end, side;


CREATE VIEW pepe_feature_f3_5s AS
SELECT
  b.window_start,
  b.window_end,
  b.side AS book_side,                 -- 'BID'/'ASK'
  b.depth_k_max,
  b.vanish_ratio,
  COALESCE(t.trade_vol, CAST(0 AS DECIMAL(38, 18))) AS trade_vol_same_side,
  COALESCE(t.trade_vol, CAST(0 AS DECIMAL(38, 18))) / NULLIF(b.depth_k_max, 0) AS exec_ratio
FROM pepe_feature_f2_5s b
LEFT JOIN pepe_trade_vol_5s t
  ON b.window_start = t.window_start
 AND b.window_end = t.window_end
 AND t.side = CASE WHEN b.side = 'BID' THEN 'B' ELSE 'S' END;


 CREATE VIEW pepe_f1_5s AS
SELECT
  window_start,
  window_end,
  side,
  MAX(depth_surge_ratio) AS max_depth_surge_ratio,
  MAX(depth_surge_abs) AS max_depth_surge_abs
FROM TABLE(
  TUMBLE(
    TABLE pepe_feature_f1_1s,
    DESCRIPTOR(window_start),
    INTERVAL '5' SECOND
  )
)
GROUP BY window_start, window_end, side;

CREATE TABLE pepe_spoofing_features_5s_sink (
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  book_side STRING,
  max_depth_surge_ratio DECIMAL(38,18),
  max_depth_surge_abs DECIMAL(38,18),
  depth_k_max DECIMAL(38,18),
  vanish_ratio DECIMAL(38,18),
  trade_vol_same_side DECIMAL(38,18),
  exec_ratio DECIMAL(38,18)
) WITH (
  'connector' = 'kafka',
  'topic' = 'ods_pepe_spoofing_features_5s',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'ods_pepe_spoofing_features_5s',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json'
);

CREATE VIEW pepe_spoofing_features_5s AS
SELECT
  f3.window_start,
  f3.window_end,
  f3.book_side,
  f1.max_depth_surge_ratio,
  f1.max_depth_surge_abs,
  f3.depth_k_max,
  f3.vanish_ratio,
  f3.trade_vol_same_side,
  f3.exec_ratio
FROM pepe_feature_f3_5s f3
JOIN pepe_f1_5s f1
  ON f3.window_start = f1.window_start
 AND f3.window_end = f1.window_end
 AND f3.book_side = f1.side;


INSERT INTO pepe_spoofing_features_5s_sink
SELECT * FROM pepe_spoofing_features_5s;