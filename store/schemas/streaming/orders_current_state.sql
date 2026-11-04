-- Delta Lake Schema: orders_current_state
-- Live materialized snapshot of active orders, current lifecycle status, and courtesy credit totals.
-- Partition strategy: status
-- Primary Key: order_id

CREATE TABLE IF NOT EXISTS orders_current_state (
    order_id STRING NOT NULL,                    -- Olist order identifier (Primary Key)
    status STRING NOT NULL,                      -- Order status (e.g., delivered, shipped, canceled)
    credit_issued BIGINT NOT NULL DEFAULT 0,     -- Courtesy credit total in integer cents (BRL)
    updated_at TIMESTAMP NOT NULL,               -- Microsecond precision last-updated timestamp
    version BIGINT NOT NULL DEFAULT 1            -- Monotonic aggregate version for optimistic concurrency
)
USING DELTA
PARTITIONED BY (status)
LOCATION 'data/delta/orders_current_state'
TBLPROPERTIES (
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '2',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
