-- Таблица фактов продаж
CREATE TABLE fact_sales (
    fact_sk           BIGSERIAL PRIMARY KEY,
    source_row_id     BIGINT NOT NULL UNIQUE REFERENCES mock_data (row_id),
    source_sale_id    INTEGER,
    date_sk           INTEGER NOT NULL REFERENCES dim_date (date_sk),
    customer_sk       INTEGER NOT NULL REFERENCES dim_customer (customer_sk),
    seller_sk         INTEGER NOT NULL REFERENCES dim_seller (seller_sk),
    product_sk        INTEGER NOT NULL REFERENCES dim_product (product_sk),
    store_sk          INTEGER NOT NULL REFERENCES dim_store (store_sk),
    supplier_sk       INTEGER NOT NULL REFERENCES dim_supplier (supplier_sk),
    sale_quantity     INTEGER NOT NULL,
    sale_total_price  NUMERIC(12, 2) NOT NULL
);

CREATE INDEX idx_fact_sales_date ON fact_sales (date_sk);
CREATE INDEX idx_fact_sales_customer ON fact_sales (customer_sk);
CREATE INDEX idx_fact_sales_product ON fact_sales (product_sk);
CREATE INDEX idx_fact_sales_store ON fact_sales (store_sk);
