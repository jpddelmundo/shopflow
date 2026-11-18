CREATE SCHEMA IF NOT EXISTS wh;

CREATE OR REPLACE TABLE wh.dim_customers AS 
    SELECT ROW_NUMBER() OVER(ORDER BY customer_id ASC) AS customer_key, *
    FROM stg.customers
    UNION ALL
    SELECT 0, -1, 'Unknown customer', NULL, 'Unknown', NULL;

CREATE OR REPLACE TABLE wh.dim_products AS
    SELECT ROW_NUMBER() OVER(ORDER BY sku ASC) AS product_key, *
    FROM stg.products;

CREATE OR REPLACE TABLE wh.dim_date AS
    SELECT strftime(d, '%Y%m%d')::INTEGER AS date_key, d::DATE AS date,
    year(d) AS year, month(d) AS month, dayname(d) AS day_name,
    isodow(d) >= 6 AS is_weekend
    FROM range(DATE '2026-01-01', DATE '2027-01-01', INTERVAL 1 DAY) AS t(d);

SELECT 'dim_customer' AS table_name, count(*) AS row_count FROM wh.dim_customers
UNION ALL
SELECT 'dim_products' AS table_name, count(*) AS row_count FROM wh.dim_products
UNION ALL
SELECT 'dim_date' AS table_name, count(*) AS row_count FROM wh.dim_date;