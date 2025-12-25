-- Example: Consume from Hyperliquid WebSocket and print to console
-- This example demonstrates the WebSocket connector consuming trades data

-- Create WebSocket source table for Hyperliquid trades
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
-- Create print sink table for testing
CREATE TABLE print_sink (
    coin STRING,
    event_time TIMESTAMP(3),
    side STRING,          -- 'BID' / 'ASK'
    level_idx INT,        -- 1..K
    px DECIMAL(38,18),
    sz DECIMAL(38,18),
    n BIGINT
) WITH (
    'connector' = 'print'
);

-- Insert data from WebSocket source to print sink
INSERT INTO print_sink
SELECT * FROM hl_l2book;

