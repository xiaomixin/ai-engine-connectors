SET sql-client.execution.result-mode=TABLEAU;
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

SELECT * FROM hl_trades;

CREATE TABLE hl_l2book (
  coin STRING,
  event_time TIMESTAMP(3),
  side STRING,          -- 'BID' / 'ASK'
  level_idx INT,        -- 1..K
  px DECIMAL(38,18),
  sz DECIMAL(38,18),
  n BIGINT
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

-- SELECT * FROM hl_l2book;


