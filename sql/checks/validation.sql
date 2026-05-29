\echo '=== PostgreSQL staging ==='
SELECT 'mock_data rows' AS check_name, COUNT(*)::TEXT AS value FROM mock_data;

\echo '=== PostgreSQL DWH ==='
SELECT 'fact_sales rows' AS check_name, COUNT(*)::TEXT AS value FROM fact_sales;
SELECT 'dim_customer' AS check_name, COUNT(*)::TEXT AS value FROM dim_customer;

\echo '=== Revenue check ==='
SELECT
    CASE
        WHEN ABS(
            (SELECT SUM(sale_total_price) FROM fact_sales)
            - (SELECT SUM(NULLIF(sale_total_price, '')::NUMERIC(12, 2)) FROM mock_data)
        ) < 0.01
        THEN 'OK'
        ELSE 'FAIL'
    END AS revenue_match;
