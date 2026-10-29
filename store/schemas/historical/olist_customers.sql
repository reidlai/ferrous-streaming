-- Delta Lake Schema: olist_customers
-- Customer demographic and geographical data. Direct customer contact details and exact addresses are purged.

CREATE TABLE IF NOT EXISTS olist_customers (
    customer_id STRING NOT NULL,
    customer_unique_id STRING NOT NULL,
    customer_zip_code_prefix INT NOT NULL,
    customer_city STRING NOT NULL,
    customer_state STRING NOT NULL
)
USING DELTA
LOCATION 'data/delta/olist_customers';
