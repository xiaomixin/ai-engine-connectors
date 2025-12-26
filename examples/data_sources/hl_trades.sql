CREATE TABLE hl_trades_source (
  coin STRING,
  side STRING,              -- 'B' (aggressive buy) or 'S' (aggressive sell)
  px DECIMAL(38, 18),
  sz DECIMAL(38, 18),
  event_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '2' SECOND
) WITH (
  'connector' = 'ws',
  'ws.url' = 'wss://api.hyperliquid.xyz/ws',
  'ws.source' = 'hyperliquid',
  'ws.channel' = 'trades',
  'ws.symbol' = 'ETH',
  'ws.queue.capacity' = '20000',
  'ws.request.batch' = '5000',
  'ws.connect.timeout.ms' = '10000'
);

CREATE TABLE hl_trades_flat_sink (
  coin STRING,
  side STRING,
  px DECIMAL(38,18),
  sz DECIMAL(38,18),
  event_time TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'ods_hl_trades_flat',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'ods_hl_trades_flat',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json'
);

INSERT INTO hl_trades_flat_sink
SELECT coin, side, px, sz, event_time 
FROM hl_trades_source;