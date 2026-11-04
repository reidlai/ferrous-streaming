-- Delta Lake Schema: olist_orders
-- Historical Olist Brazilian E-Commerce orders dataset for analytical queries.

CREATE TABLE IF NOT EXISTS olist_orders (
    order_id STRING NOT NULL,
    customer_id STRING NOT NULL,
    order_status STRING NOT NULL,
    order_purchase_timestamp TIMESTAMP NOT NULL,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (order_status)
LOCATION 'data/delta/olist_orders';
