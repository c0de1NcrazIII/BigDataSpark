"""Spark ETL: mock_data (PostgreSQL) -> модель «снежинка» в PostgreSQL."""

from pyspark.sql.functions import (
    col,
    dayofmonth,
    lit,
    month,
    quarter,
    when,
    year,
)

from common.pg_utils import read_pg, truncate_dwh, write_pg
from common.session import get_spark
from common.transforms import clean_text, first_by, null_if_blank, parse_sale_date


def main() -> None:
    spark = get_spark("01_load_dwh")
    truncate_dwh(spark)

    src = read_pg(spark, "mock_data")

    countries = (
        src.select(clean_text(col("customer_country")).alias("country_name"))
        .union(src.select(clean_text(col("seller_country")).alias("country_name")))
        .union(src.select(clean_text(col("store_country")).alias("country_name")))
        .union(src.select(clean_text(col("supplier_country")).alias("country_name")))
        .filter(col("country_name").isNotNull() & (col("country_name") != ""))
        .distinct()
    )
    write_pg(countries, "dim_country")
    dim_country = read_pg(spark, "dim_country")

    store_cities = src.select(
        clean_text(col("store_city")).alias("city_name"),
        null_if_blank(col("store_state")).alias("state_name"),
        clean_text(col("store_country")).alias("country_name"),
    )
    supplier_cities = src.select(
        clean_text(col("supplier_city")).alias("city_name"),
        lit(None).cast("string").alias("state_name"),
        clean_text(col("supplier_country")).alias("country_name"),
    )
    cities = (
        store_cities.unionByName(supplier_cities)
        .filter(col("city_name").isNotNull() & (col("city_name") != ""))
        .distinct()
        .join(dim_country, "country_name", "inner")
        .select("city_name", "state_name", "country_sk")
        .distinct()
    )
    write_pg(cities, "dim_city")
    dim_city = read_pg(spark, "dim_city")
    dim_country = read_pg(spark, "dim_country")

    categories = (
        src.select(
            clean_text(col("product_category")).alias("product_category"),
            null_if_blank(col("pet_category")).alias("pet_category"),
        )
        .filter(col("product_category").isNotNull() & (col("product_category") != ""))
        .distinct()
    )
    write_pg(categories, "dim_product_category")
    dim_product_category = read_pg(spark, "dim_product_category")

    dates = (
        src.filter(col("sale_date").isNotNull() & (clean_text(col("sale_date")) != ""))
        .select(parse_sale_date(col("sale_date")).alias("full_date"))
        .distinct()
        .withColumn("year_num", year(col("full_date")).cast("short"))
        .withColumn("month_num", month(col("full_date")).cast("short"))
        .withColumn("day_num", dayofmonth(col("full_date")).cast("short"))
        .withColumn("quarter_num", quarter(col("full_date")).cast("short"))
    )
    write_pg(dates, "dim_date")
    dim_date = read_pg(spark, "dim_date")

    customers = first_by(
        src.join(
            dim_country,
            dim_country.country_name == clean_text(col("customer_country")),
            "left",
        ).select(
            null_if_blank(col("sale_customer_id")).cast("int").alias("source_customer_id"),
            col("customer_first_name").alias("first_name"),
            col("customer_last_name").alias("last_name"),
            null_if_blank(col("customer_age")).cast("int").alias("age"),
            col("customer_email").alias("email"),
            null_if_blank(col("customer_postal_code")).alias("postal_code"),
            col("customer_pet_type").alias("pet_type"),
            col("customer_pet_name").alias("pet_name"),
            col("customer_pet_breed").alias("pet_breed"),
            col("country_sk"),
            col("row_id"),
            col("customer_email"),
        ),
        ["customer_email"],
    ).drop("customer_email", "row_id")
    write_pg(customers, "dim_customer")
    dim_customer = read_pg(spark, "dim_customer")

    sellers = first_by(
        src.join(
            dim_country,
            dim_country.country_name == clean_text(col("seller_country")),
            "left",
        ).select(
            null_if_blank(col("sale_seller_id")).cast("int").alias("source_seller_id"),
            col("seller_first_name").alias("first_name"),
            col("seller_last_name").alias("last_name"),
            col("seller_email").alias("email"),
            null_if_blank(col("seller_postal_code")).alias("postal_code"),
            col("country_sk"),
            col("row_id"),
            col("seller_email"),
        ),
        ["seller_email"],
    ).drop("seller_email", "row_id")
    write_pg(sellers, "dim_seller")
    dim_seller = read_pg(spark, "dim_seller")

    dpc = dim_product_category.alias("dpc")
    m = src.alias("m")
    products = first_by(
        m.join(
            dpc,
            (col("dpc.product_category") == clean_text(col("m.product_category")))
            & (
                col("dpc.pet_category").eqNullSafe(null_if_blank(col("m.pet_category")))
            ),
            "inner",
        ).select(
            null_if_blank(col("m.sale_product_id")).cast("int").alias("source_product_id"),
            col("m.product_name").alias("product_name"),
            col("dpc.category_sk").alias("category_sk"),
            null_if_blank(col("m.product_price")).cast("decimal(12,2)").alias("list_price"),
            null_if_blank(col("m.product_quantity")).cast("int").alias("stock_quantity"),
            null_if_blank(col("m.product_weight")).cast("decimal(12,2)").alias("weight"),
            col("m.product_color").alias("color"),
            col("m.product_size").alias("size_label"),
            col("m.product_brand").alias("brand"),
            col("m.product_material").alias("material"),
            col("m.product_description").alias("description"),
            null_if_blank(col("m.product_rating")).cast("decimal(4,2)").alias("rating"),
            null_if_blank(col("m.product_reviews")).cast("int").alias("reviews_count"),
            when(
                clean_text(col("m.product_release_date")) != "",
                parse_sale_date(col("m.product_release_date")),
            )
            .otherwise(lit(None))
            .alias("release_date"),
            when(
                clean_text(col("m.product_expiry_date")) != "",
                parse_sale_date(col("m.product_expiry_date")),
            )
            .otherwise(lit(None))
            .alias("expiry_date"),
            col("m.row_id").alias("row_id"),
            col("m.sale_product_id").alias("sale_product_id"),
            col("m.product_price").alias("product_price"),
            col("m.product_brand").alias("product_brand"),
            col("m.product_material").alias("product_material"),
            col("m.product_color").alias("product_color"),
            col("m.product_size").alias("product_size"),
        ),
        [
            "sale_product_id",
            "product_name",
            "category_sk",
            "product_price",
            "product_brand",
            "product_material",
            "product_color",
            "product_size",
        ],
    ).drop(
        "row_id",
        "sale_product_id",
        "product_price",
        "product_brand",
        "product_material",
        "product_color",
        "product_size",
    )
    write_pg(products, "dim_product")
    dim_product = read_pg(spark, "dim_product")

    stores = first_by(
        src.join(
            dim_country.alias("co_store"),
            col("co_store.country_name") == clean_text(col("store_country")),
            "inner",
        )
        .join(
            dim_city.alias("ci_store"),
            (col("ci_store.city_name") == clean_text(col("store_city")))
            & (col("ci_store.country_sk") == col("co_store.country_sk"))
            & (col("ci_store.state_name").eqNullSafe(null_if_blank(col("store_state")))),
            "inner",
        )
        .select(
            col("store_name"),
            col("store_location"),
            col("ci_store.city_sk").alias("city_sk"),
            col("store_phone").alias("phone"),
            col("store_email").alias("email"),
            col("row_id"),
            col("store_email"),
        ),
        ["store_email"],
    ).drop("store_email", "row_id")
    write_pg(stores, "dim_store")
    dim_store = read_pg(spark, "dim_store")

    suppliers = first_by(
        src.join(
            dim_country.alias("co"),
            col("co.country_name") == clean_text(col("supplier_country")),
            "inner",
        )
        .join(
            dim_city.alias("ci"),
            (col("ci.city_name") == clean_text(col("supplier_city")))
            & (col("ci.country_sk") == col("co.country_sk"))
            & col("ci.state_name").isNull(),
            "left",
        )
        .select(
            col("supplier_name"),
            col("supplier_contact").alias("contact_name"),
            col("supplier_email").alias("email"),
            col("supplier_phone").alias("phone"),
            col("supplier_address").alias("address"),
            col("ci.city_sk").alias("city_sk"),
            col("co.country_sk").alias("country_sk"),
            col("row_id"),
            col("supplier_email"),
        ),
        ["supplier_email"],
    ).drop("supplier_email", "row_id")
    write_pg(suppliers, "dim_supplier")

    dim_product_category = read_pg(spark, "dim_product_category").alias("dpc_fact")
    dim_product = read_pg(spark, "dim_product").alias("dp")
    dim_supplier = read_pg(spark, "dim_supplier")

    mf = src.alias("mf")
    facts = (
        mf.join(dim_date, dim_date.full_date == parse_sale_date(col("mf.sale_date")), "inner")
        .join(dim_customer, dim_customer.email == col("mf.customer_email"), "inner")
        .join(dim_seller, dim_seller.email == col("mf.seller_email"), "inner")
        .join(
            dim_product_category,
            (col("dpc_fact.product_category") == clean_text(col("mf.product_category")))
            & (
                col("dpc_fact.pet_category").eqNullSafe(
                    null_if_blank(col("mf.pet_category"))
                )
            ),
            "inner",
        )
        .join(
            dim_product,
            (col("dp.source_product_id") == null_if_blank(col("mf.sale_product_id")).cast("int"))
            & (col("dp.product_name") == col("mf.product_name"))
            & (col("dp.category_sk") == col("dpc_fact.category_sk"))
            & (
                col("dp.list_price")
                == null_if_blank(col("mf.product_price")).cast("decimal(12,2)")
            )
            & col("dp.brand").eqNullSafe(col("mf.product_brand"))
            & col("dp.material").eqNullSafe(col("mf.product_material"))
            & col("dp.color").eqNullSafe(col("mf.product_color"))
            & col("dp.size_label").eqNullSafe(col("mf.product_size")),
            "inner",
        )
        .join(dim_store, dim_store.email == col("mf.store_email"), "inner")
        .join(dim_supplier, dim_supplier.email == col("mf.supplier_email"), "inner")
        .select(
            col("mf.row_id").alias("source_row_id"),
            null_if_blank(col("mf.id")).cast("int").alias("source_sale_id"),
            col("date_sk"),
            col("customer_sk"),
            col("seller_sk"),
            col("dp.product_sk").alias("product_sk"),
            col("store_sk"),
            col("supplier_sk"),
            null_if_blank(col("mf.sale_quantity")).cast("int").alias("sale_quantity"),
            null_if_blank(col("mf.sale_total_price")).cast("decimal(12,2)").alias("sale_total_price"),
        )
    )
    write_pg(facts, "fact_sales")

    fact_count = read_pg(spark, "fact_sales").count()
    print(f"DWH load completed: fact_sales rows = {fact_count}")
    spark.stop()


if __name__ == "__main__":
    main()
