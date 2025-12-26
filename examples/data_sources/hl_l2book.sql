CREATE TABLE hl_l2book_source (
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

CREATE TABLE hl_l2book_flat_sink (
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

INSERT INTO hl_l2book_flat_sink
SELECT coin, side, level_idx, px, sz, n, event_time 
FROM hl_l2book_source;

