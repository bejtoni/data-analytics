-- 📦 ARCHIVE SCHEMA (SCD2)
CREATE SCHEMA IF NOT EXISTS archive;

CREATE TABLE IF NOT EXISTS archive.customers (
    customer_id VARCHAR,
    customer_unique_id VARCHAR,
    customer_zip_code_prefix INTEGER,
    customer_city VARCHAR,
    customer_state VARCHAR,
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP NOT NULL,
    updated TIMESTAMP,
    process TEXT,
    PRIMARY KEY (customer_id, start_date)
);

CREATE TABLE IF NOT EXISTS archive.sellers (
    seller_id VARCHAR,
    seller_zip_code_prefix INTEGER,
    seller_city VARCHAR,
    seller_state VARCHAR,
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP NOT NULL,
    updated TIMESTAMP,
    process TEXT,
    PRIMARY KEY (seller_id, start_date)
);

CREATE TABLE IF NOT EXISTS archive.products (
    product_id VARCHAR,
    product_category_name VARCHAR,
    product_name_lenght DECIMAL,
    product_description_lenght DECIMAL,
    product_photos_qty DECIMAL,
    product_weight_g DECIMAL,
    product_length_cm DECIMAL,
    product_height_cm DECIMAL,
    product_width_cm DECIMAL,
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP NOT NULL,
    updated TIMESTAMP,
    process TEXT,
    PRIMARY KEY (product_id, start_date)
);

CREATE TABLE IF NOT EXISTS archive.orders (
    order_id VARCHAR,
    customer_id VARCHAR,
    order_status VARCHAR,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP NOT NULL,
    updated TIMESTAMP,
    process TEXT,
    PRIMARY KEY (order_id, start_date)
);

CREATE TABLE IF NOT EXISTS archive.order_reviews (
    review_id VARCHAR,
    order_id VARCHAR,
    review_score INTEGER,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP,
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP NOT NULL,
    updated TIMESTAMP,
    process TEXT,
    PRIMARY KEY (review_id, start_date)
);

CREATE TABLE IF NOT EXISTS archive.order_payments (
    order_id VARCHAR,
    payment_sequential INTEGER,
    payment_type VARCHAR,
    payment_installments INTEGER,
    payment_value DECIMAL,
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP NOT NULL,
    updated TIMESTAMP,
    process TEXT,
    PRIMARY KEY (order_id, payment_sequential, start_date)
);

CREATE TABLE IF NOT EXISTS archive.exchange_rates (
    date TIMESTAMP,
    base_currency VARCHAR,
    target_currency VARCHAR,
    rate DECIMAL,
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP NOT NULL,
    updated TIMESTAMP,
    process TEXT,
    PRIMARY KEY (date, base_currency, target_currency, start_date)
);

CREATE TABLE IF NOT EXISTS archive.order_items (
    order_id VARCHAR,
    order_item_id INTEGER,
    product_id VARCHAR,
    seller_id VARCHAR,
    shipping_limit_date TIMESTAMP,
    price DECIMAL,
    freight_value DECIMAL,
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP NOT NULL,
    updated TIMESTAMP,
    process TEXT,
    PRIMARY KEY (order_id, order_item_id, start_date)
);

