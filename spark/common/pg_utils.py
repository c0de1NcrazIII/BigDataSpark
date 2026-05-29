from pyspark.sql import DataFrame, SparkSession

from common.config import pg_jdbc_url, pg_properties


def exec_pg(spark: SparkSession, sql: str) -> None:
    props = pg_properties()
    spark._jvm.Class.forName(props["driver"])
    conn = spark._jvm.java.sql.DriverManager.getConnection(
        pg_jdbc_url(), props["user"], props["password"]
    )
    try:
        stmt = conn.createStatement()
        stmt.execute(sql)
        stmt.close()
    finally:
        conn.close()


def read_pg(spark: SparkSession, table_or_query: str) -> DataFrame:
    if table_or_query.strip().startswith("("):
        dbtable = table_or_query
    else:
        dbtable = table_or_query
    return spark.read.jdbc(pg_jdbc_url(), dbtable, properties=pg_properties())


def write_pg(df: DataFrame, table: str, mode: str = "append") -> None:
    (
        df.write.format("jdbc")
        .option("url", pg_jdbc_url())
        .option("dbtable", table)
        .option("user", pg_properties()["user"])
        .option("password", pg_properties()["password"])
        .option("driver", pg_properties()["driver"])
        .mode(mode)
        .save()
    )


def truncate_dwh(spark: SparkSession) -> None:
    exec_pg(
        spark,
        """
        TRUNCATE TABLE
            fact_sales,
            dim_supplier,
            dim_store,
            dim_product,
            dim_seller,
            dim_customer,
            dim_date,
            dim_product_category,
            dim_city,
            dim_country
        RESTART IDENTITY CASCADE
        """,
    )
