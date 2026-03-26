-- =========================================
-- PHASE 1: DATA PROFILING
-- =========================================
-- Sample raw data
SELECT *
FROM C01_l01_ecommerce_retail_data_table
LIMIT 20;
-- Check nulls
SELECT COUNT(*) AS total_rows,
    SUM(
        CASE
            WHEN date IS NULL THEN 1
            ELSE 0
        END
    ) AS null_date,
...-- Check customer segment quality (detect typos)
SELECT customer_segment,
    COUNT(*)
FROM C01_l01_ecommerce_retail_data_table
GROUP BY customer_segment;
-- Check payment methods distribution
SELECT payment_method,
    COUNT(*)
FROM C01_l01_ecommerce_retail_data_table
GROUP BY payment_method;



-- =========================================
-- PHASE 2: PARSING & TYPING (SILVER)
-- =========================================
-- Parse date and standardise types
CREATE OR REPLACE TEMP VIEW silver_parsed AS
SELECT row_id,
    COALESCE(
        try_strptime(replace(date, '.', '-'), '%Y-%m-%d'),
        try_strptime(replace(date, '.', '-'), '%d-%m-%Y')
    ) AS parsed_dt,
    lower(trim(customer_segment)) AS customer_segment_raw,
    try_cast(order_amount_old AS DOUBLE) AS order_amount_old,
    try_cast(cost AS DOUBLE) AS cost,
    try_cast(is_return AS INTEGER) AS is_return,
    payment_method,
    try_cast(hour_of_day AS INTEGER) AS hour_of_day
FROM C01_l01_ecommerce_retail_data_table;
-- Check parsing success
SELECT COUNT(*) AS total_rows,
    SUM(
        CASE
            WHEN parsed_dt IS NULL THEN 1
            ELSE 0
        END
    ) AS date_parse_failures
FROM silver_parsed;



-- =========================================
-- PHASE 3: DATA NORMALISATION
-- =========================================
-- Customer segment typos and standardise date format
CREATE OR REPLACE TEMP VIEW silver_normalised AS
SELECT row_id,
    parsed_dt,
    strftime(parsed_dt::DATE, '%d-%m-%Y') AS date,
    CASE
        WHEN customer_segment_raw IS NULL THEN NULL
        WHEN regexp_replace(customer_segment_raw, '[^a-z]', '') IN ('standrad') THEN 'standard'
        WHEN regexp_replace(customer_segment_raw, '[^a-z]', '') IN ('premuim') THEN 'premium'
        WHEN regexp_replace(customer_segment_raw, '[^a-z]', '') IN ('platnum') THEN 'platinum'
        ELSE customer_segment_raw
    END AS customer_segment,
    order_amount_old,
    cost,
    is_return,
    payment_method,
    hour_of_day
FROM silver_parsed;
-- Check cleaned segments
SELECT customer_segment,
    COUNT(*)
FROM silver_normalised
GROUP BY customer_segment;



-- =========================================
-- PHASE 4: BUSINESS RULES & FILTERING
-- =========================================
-- Diagnose invalid records
SELECT SUM(
        CASE
            WHEN parsed_dt IS NULL THEN 1
            ELSE 0
        END
    ) AS bad_date,
    SUM(
        CASE
            WHEN order_amount_old IS NULL
            OR order_amount_old < 5 THEN 1
            ELSE 0
        END
    ) AS bad_amount,
    SUM(
        CASE
            WHEN cost IS NULL
            OR cost <= 0 THEN 1
            ELSE 0
        END
    ) AS bad_cost,
    SUM(
        CASE
            WHEN is_return NOT IN (0, 1) THEN 1
            ELSE 0
        END
    ) AS bad_return_flag,
    SUM(
        CASE
            WHEN hour_of_day NOT BETWEEN 0 AND 23 THEN 1
            ELSE 0
        END
    ) AS bad_hour
FROM silver_normalised;
-- Apply business rules
CREATE OR REPLACE TEMP VIEW silver_filtered AS
SELECT *
FROM silver_normalised
WHERE parsed_dt IS NOT NULL
    AND order_amount_old >= 5
    AND cost > 0
    AND is_return IS NOT NULL
    AND hour_of_day BETWEEN 0 AND 23;
-- Check retained rows
SELECT COUNT(*)
FROM silver_filtered;



-- =========================================
-- PHASE 5: DEDUPLICATION (GOLD LAYER)
-- =========================================
-- Count distinct rows before deduplication
SELECT COUNT(*)
FROM (
        SELECT DISTINCT *
        FROM silver_filtered
    );
-- Remove duplicates
CREATE OR REPLACE TEMP VIEW clean_table AS
SELECT DISTINCT row_id,
    date,
    customer_segment,
    order_amount_old,
    cost,
    is_return,
    payment_method,
    hour_of_day
FROM silver_filtered;
-- Final check
SELECT COUNT(*) AS final_rows
FROM clean_table;