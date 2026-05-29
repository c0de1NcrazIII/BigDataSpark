CREATE TABLE IF NOT EXISTS petshop.mart_product_sales (
    section String,
    product_sk Nullable(Int32),
    product_name Nullable(String),
    product_category Nullable(String),
    units_sold Nullable(Int64),
    revenue Nullable(Decimal(18, 2)),
    avg_rating Nullable(Float32),
    reviews_count Nullable(Int32),
    rank_by_units Nullable(UInt32)
) ENGINE = MergeTree ORDER BY (section);

CREATE TABLE IF NOT EXISTS petshop.mart_customer_sales (
    section String,
    customer_sk Nullable(Int32),
    customer_email Nullable(String),
    customer_country Nullable(String),
    total_revenue Nullable(Decimal(18, 2)),
    orders_count Nullable(Int64),
    avg_check Nullable(Decimal(18, 2)),
    rank_by_revenue Nullable(UInt32),
    customers_in_country Nullable(Int64)
) ENGINE = MergeTree ORDER BY (section);

CREATE TABLE IF NOT EXISTS petshop.mart_time_sales (
    section String,
    year_num Nullable(Int16),
    month_num Nullable(Int8),
    period_label Nullable(String),
    revenue Nullable(Decimal(18, 2)),
    orders_count Nullable(Int64),
    avg_order_size Nullable(Decimal(18, 2))
) ENGINE = MergeTree ORDER BY (section);

CREATE TABLE IF NOT EXISTS petshop.mart_store_sales (
    section String,
    store_sk Nullable(Int32),
    store_name Nullable(String),
    store_city Nullable(String),
    store_country Nullable(String),
    revenue Nullable(Decimal(18, 2)),
    orders_count Nullable(Int64),
    avg_check Nullable(Decimal(18, 2)),
    rank_by_revenue Nullable(UInt32)
) ENGINE = MergeTree ORDER BY (section);

CREATE TABLE IF NOT EXISTS petshop.mart_supplier_sales (
    section String,
    supplier_sk Nullable(Int32),
    supplier_name Nullable(String),
    supplier_country Nullable(String),
    revenue Nullable(Decimal(18, 2)),
    avg_product_price Nullable(Decimal(18, 2)),
    orders_count Nullable(Int64),
    rank_by_revenue Nullable(UInt32)
) ENGINE = MergeTree ORDER BY (section);

CREATE TABLE IF NOT EXISTS petshop.mart_product_quality (
    section String,
    product_sk Nullable(Int32),
    product_name Nullable(String),
    rating Nullable(Float32),
    reviews_count Nullable(Int32),
    units_sold Nullable(Int64),
    revenue Nullable(Decimal(18, 2)),
    rank_by_rating Nullable(UInt32),
    rank_by_reviews Nullable(UInt32)
) ENGINE = MergeTree ORDER BY (section);
