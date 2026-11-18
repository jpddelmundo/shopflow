CREATE SCHEMA IF NOT EXISTS stg;

CREATE OR REPLACE TABLE stg.customers AS
    SELECT DISTINCT customer_id::INT AS customer_id,
        name,
        email,
        city: concat(upper(trim(city)[1]), lower(trim(city)[2:])),
        date_added::DATE AS date_added
    FROM "sources/customers.csv";

CREATE OR REPLACE TABLE stg.orders AS
    SELECT CAST(order_id AS VARCHAR) AS order_id,
        customer_id::INTEGER AS customer_id,
        ordered_at::DATETIME AS ordered_at,
        items,
        status
    FROM read_json('data/raw/orders/*/orders.jsonl', hive_partitioning=true);

CREATE OR REPLACE TABLE stg.products AS
    SELECT id::INT AS id,
        nullif(name, '') AS name,
        nullif(sku, '') AS sku,
        nullif(category, '') AS category,
        price::DECIMAL(18, 2) AS price
    FROM "sources/products.json";