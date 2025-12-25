-- Example: Consume from Hyperliquid WebSocket and print to console
-- This example demonstrates the WebSocket connector consuming trades data

-- Create WebSocket source table for Hyperliquid trades
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
  'ws.queue.capacity' = '50000',
  'ws.request.batch' = '2000'
);
-- Create print sink table for testing
CREATE TABLE print_sink (
    source STRING,
    channel STRING,
    symbol STRING,
    event_time_ms BIGINT,
    payload STRING
) WITH (
    'connector' = 'print'
);

-- Insert data from WebSocket source to print sink
INSERT INTO print_sink
SELECT * FROM hl_l2book;

