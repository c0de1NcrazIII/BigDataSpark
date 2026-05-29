SELECT 'mart_product_sales' AS mart, count() AS rows FROM petshop.mart_product_sales;
SELECT 'mart_customer_sales' AS mart, count() AS rows FROM petshop.mart_customer_sales;
SELECT 'mart_time_sales' AS mart, count() AS rows FROM petshop.mart_time_sales;
SELECT 'mart_store_sales' AS mart, count() AS rows FROM petshop.mart_store_sales;
SELECT 'mart_supplier_sales' AS mart, count() AS rows FROM petshop.mart_supplier_sales;
SELECT 'mart_product_quality' AS mart, count() AS rows FROM petshop.mart_product_quality;

SELECT section, count() AS rows
FROM petshop.mart_product_sales
GROUP BY section
ORDER BY section;

SELECT section, count() AS rows
FROM petshop.mart_customer_sales
GROUP BY section
ORDER BY section;
