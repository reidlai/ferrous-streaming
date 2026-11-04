-- Delta Lake Schema: olist_reviews
-- Customer reviews and sentiment ratings. All direct PII is excluded.

CREATE TABLE IF NOT EXISTS olist_reviews (
    review_id STRING NOT NULL,
    order_id STRING NOT NULL,
    review_score INT NOT NULL,
    review_comment_title STRING,
    review_comment_message STRING,
    review_creation_date TIMESTAMP NOT NULL,
    review_answer_timestamp TIMESTAMP NOT NULL
)
USING DELTA
LOCATION 'data/delta/olist_reviews';
