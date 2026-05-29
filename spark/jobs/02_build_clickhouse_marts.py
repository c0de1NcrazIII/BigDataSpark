"""Spark: витрины (6 отчётов) из PostgreSQL DWH в ClickHouse."""

from pyspark.sql.functions import (
    avg,
    col,
    concat_ws,
    count,
    countDistinct,
    desc,
    lit,
    rank,
    round,
    sum,
)
from pyspark.sql.window import Window

from common.ch_utils import write_ch
from common.pg_utils import read_pg
from common.session import get_spark

SALES_QUERY = """
(
SELECT
    f.fact_sk,
    f.sale_quantity,
    f.sale_total_price,
    d.year_num,
    d.month_num,
    d.full_date,
    cu.customer_sk,
    cu.email AS customer_email,
    cu.first_name AS customer_first_name,
    cu.last_name AS customer_last_name,
    co_cu.country_name AS customer_country,
    pr.product_sk,
    pr.product_name,
    pr.rating AS product_rating,
    pr.reviews_count,
    pr.list_price,
    pc.product_category,
    st.store_sk,
    st.store_name,
    ci.city_name AS store_city,
    co_st.country_name AS store_country,
    su.supplier_sk,
    su.supplier_name,
    co_su.country_name AS supplier_country
FROM fact_sales f
JOIN dim_date d ON d.date_sk = f.date_sk
JOIN dim_customer cu ON cu.customer_sk = f.customer_sk
LEFT JOIN dim_country co_cu ON co_cu.country_sk = cu.country_sk
JOIN dim_seller se ON se.seller_sk = f.seller_sk
JOIN dim_product pr ON pr.product_sk = f.product_sk
JOIN dim_product_category pc ON pc.category_sk = pr.category_sk
JOIN dim_store st ON st.store_sk = f.store_sk
JOIN dim_city ci ON ci.city_sk = st.city_sk
JOIN dim_country co_st ON co_st.country_sk = ci.country_sk
JOIN dim_supplier su ON su.supplier_sk = f.supplier_sk
LEFT JOIN dim_country co_su ON co_su.country_sk = su.country_sk
) sales
"""


def main() -> None:
    spark = get_spark("02_build_clickhouse_marts")
    sales = read_pg(spark, SALES_QUERY)

    # 1. Витрина продаж по продуктам
    product_agg = sales.groupBy(
        "product_sk", "product_name", "product_category", "product_rating", "reviews_count"
    ).agg(
        sum("sale_quantity").alias("units_sold"),
        round(sum("sale_total_price"), 2).alias("revenue"),
    )
    w_prod = Window.orderBy(col("units_sold").desc())
    mart_products = (
        product_agg.withColumn("rank_by_units", rank().over(w_prod))
        .withColumn("section", lit("product_detail"))
        .select(
            "section",
            "product_sk",
            "product_name",
            "product_category",
            "units_sold",
            "revenue",
            col("product_rating").alias("avg_rating"),
            "reviews_count",
            "rank_by_units",
        )
    )
    category_agg = (
        sales.groupBy("product_category")
        .agg(round(sum("sale_total_price"), 2).alias("revenue"))
        .withColumn("section", lit("category_revenue"))
        .select(
            "section",
            lit(None).cast("int").alias("product_sk"),
            lit(None).cast("string").alias("product_name"),
            col("product_category"),
            lit(None).cast("long").alias("units_sold"),
            "revenue",
            lit(None).cast("float").alias("avg_rating"),
            lit(None).cast("int").alias("reviews_count"),
            lit(None).cast("int").alias("rank_by_units"),
        )
    )
    write_ch(mart_products.unionByName(category_agg), "mart_product_sales")

    # 2. Витрина продаж по клиентам
    customer_agg = sales.groupBy(
        "customer_sk", "customer_email", "customer_country"
    ).agg(
        round(sum("sale_total_price"), 2).alias("total_revenue"),
        count("fact_sk").alias("orders_count"),
        round(avg("sale_total_price"), 2).alias("avg_check"),
    )
    w_cust = Window.orderBy(col("total_revenue").desc())
    mart_customers = (
        customer_agg.withColumn("rank_by_revenue", rank().over(w_cust))
        .withColumn("section", lit("customer_detail"))
        .select(
            "section",
            "customer_sk",
            "customer_email",
            col("customer_country"),
            "total_revenue",
            "orders_count",
            "avg_check",
            "rank_by_revenue",
            lit(None).cast("long").alias("customers_in_country"),
        )
    )
    country_agg = (
        sales.groupBy("customer_country")
        .agg(
            countDistinct("customer_sk").alias("customers_in_country"),
            round(sum("sale_total_price"), 2).alias("total_revenue"),
        )
        .withColumn("section", lit("country_distribution"))
        .select(
            "section",
            lit(None).cast("int").alias("customer_sk"),
            lit(None).cast("string").alias("customer_email"),
            col("customer_country"),
            "total_revenue",
            lit(None).cast("long").alias("orders_count"),
            lit(None).cast("decimal(18,2)").alias("avg_check"),
            lit(None).cast("int").alias("rank_by_revenue"),
            "customers_in_country",
        )
    )
    write_ch(mart_customers.unionByName(country_agg), "mart_customer_sales")

    # 3. Витрина продаж по времени
    monthly = (
        sales.groupBy("year_num", "month_num")
        .agg(
            round(sum("sale_total_price"), 2).alias("revenue"),
            count("fact_sk").alias("orders_count"),
            round(avg("sale_total_price"), 2).alias("avg_order_size"),
        )
        .withColumn(
            "period_label",
            concat_ws("-", col("year_num").cast("string"), col("month_num").cast("string")),
        )
        .withColumn("section", lit("monthly_trend"))
        .select(
            "section",
            "year_num",
            "month_num",
            "period_label",
            "revenue",
            "orders_count",
            "avg_order_size",
        )
    )
    yearly = (
        sales.groupBy("year_num")
        .agg(
            round(sum("sale_total_price"), 2).alias("revenue"),
            count("fact_sk").alias("orders_count"),
            round(avg("sale_total_price"), 2).alias("avg_order_size"),
        )
        .withColumn("month_num", lit(None).cast("byte"))
        .withColumn("period_label", col("year_num").cast("string"))
        .withColumn("section", lit("yearly_trend"))
        .select(
            "section",
            "year_num",
            "month_num",
            "period_label",
            "revenue",
            "orders_count",
            "avg_order_size",
        )
    )
    write_ch(monthly.unionByName(yearly), "mart_time_sales")

    # 4. Витрина продаж по магазинам
    store_agg = sales.groupBy(
        "store_sk", "store_name", "store_city", "store_country"
    ).agg(
        round(sum("sale_total_price"), 2).alias("revenue"),
        count("fact_sk").alias("orders_count"),
        round(avg("sale_total_price"), 2).alias("avg_check"),
    )
    w_store = Window.orderBy(col("revenue").desc())
    mart_stores = (
        store_agg.withColumn("rank_by_revenue", rank().over(w_store))
        .withColumn("section", lit("store_detail"))
        .select(
            "section",
            "store_sk",
            "store_name",
            "store_city",
            "store_country",
            "revenue",
            "orders_count",
            "avg_check",
            "rank_by_revenue",
        )
    )
    write_ch(mart_stores, "mart_store_sales")

    # 5. Витрина продаж по поставщикам
    supplier_agg = sales.groupBy("supplier_sk", "supplier_name", "supplier_country").agg(
        round(sum("sale_total_price"), 2).alias("revenue"),
        round(avg("list_price"), 2).alias("avg_product_price"),
        count("fact_sk").alias("orders_count"),
    )
    w_sup = Window.orderBy(col("revenue").desc())
    mart_suppliers = (
        supplier_agg.withColumn("rank_by_revenue", rank().over(w_sup))
        .withColumn("section", lit("supplier_detail"))
        .select(
            "section",
            "supplier_sk",
            "supplier_name",
            "supplier_country",
            "revenue",
            "avg_product_price",
            "orders_count",
            "rank_by_revenue",
        )
    )
    write_ch(mart_suppliers, "mart_supplier_sales")

    # 6. Витрина качества продукции
    quality = sales.groupBy("product_sk", "product_name", "product_rating", "reviews_count").agg(
        sum("sale_quantity").alias("units_sold"),
        round(sum("sale_total_price"), 2).alias("revenue"),
    )
    w_rating = Window.orderBy(col("product_rating").desc_nulls_last())
    w_reviews = Window.orderBy(col("reviews_count").desc_nulls_last())
    mart_quality = (
        quality.withColumn("rank_by_rating", rank().over(w_rating))
        .withColumn("rank_by_reviews", rank().over(w_reviews))
        .withColumn("section", lit("product_quality"))
        .select(
            "section",
            "product_sk",
            "product_name",
            col("product_rating").alias("rating"),
            "reviews_count",
            "units_sold",
            "revenue",
            "rank_by_rating",
            "rank_by_reviews",
        )
    )
    write_ch(mart_quality, "mart_product_quality")

    print("ClickHouse marts built: 6 tables in database petshop")
    spark.stop()


if __name__ == "__main__":
    main()
