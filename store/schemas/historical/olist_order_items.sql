-- Delta Lake Schema: olist_order_items
-- Line item breakdown for historical orders.

CREATE TABLE IF NOT EXISTS olist_order_items (
    order_id STRING NOT NULL,
    order_item_id INT NOT NULL,
    product_id STRING NOT NULL,
    seller_id STRING NOT NULL,
    shipping_limit_date TIMESTAMP NOT NULL,
    price DOUBLE NOT NULL,
    freight_value DOUBLE NOT NULL
)
USING DELTA
LOCATION 'data/delta/olist_order_items';
