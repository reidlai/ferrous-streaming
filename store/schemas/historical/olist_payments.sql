-- Delta Lake Schema: olist_payments
-- Payment method breakdown and installment details. Direct credit card numbers are excluded.

CREATE TABLE IF NOT EXISTS olist_payments (
    order_id STRING NOT NULL,
    payment_sequential INT NOT NULL,
    payment_type STRING NOT NULL,
    payment_installments INT NOT NULL,
    payment_value DOUBLE NOT NULL
)
USING DELTA
LOCATION 'data/delta/olist_payments';
