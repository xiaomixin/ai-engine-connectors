SET sql-client.execution.result-mode=TABLEAU;
CREATE TABLE hl_l2book (
  source STRING,
  channel STRING,
  symbol STRING,
  event_time_ms BIGINT,
  payload STRING
) WITH (
  'connector' = 'ws',
  'ws.url' = 'wss://api.hyperliquid.xyz/ws',
  'ws.source' = 'hyperliquid',
  'ws.channel' = 'l2Book',
  'ws.symbol' = 'ETH',
  'ws.queue.capacity' = '50',
  'ws.request.batch' = '20'
);

SELECT * FROM hl_l2book;

