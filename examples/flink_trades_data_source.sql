CREATE TABLE hl_trades (
  coin STRING,
  event_time TIMESTAMP(3),
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

CREATE TABLE print_sink_trades (
  coin STRING,
  event_time TIMESTAMP(3),
  side STRING,
  px DECIMAL(38,18),
  sz DECIMAL(38,18)
) WITH (
  'connector' = 'print'
);

INSERT INTO print_sink_trades
SELECT *
FROM hl_trades;