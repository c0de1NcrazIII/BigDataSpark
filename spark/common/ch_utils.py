from pyspark.sql import DataFrame, SparkSession

from common.config import ch_jdbc_url, ch_properties


def _table(name: str) -> str:
    return name if "." in name else f"petshop.{name}"


def exec_ch(spark: SparkSession, sql: str) -> None:
    props = ch_properties()
    spark._jvm.Class.forName(props["driver"])
    conn = spark._jvm.java.sql.DriverManager.getConnection(
        ch_jdbc_url(), props["user"], props["password"]
    )
    try:
        stmt = conn.createStatement()
        stmt.execute(sql)
        stmt.close()
    finally:
        conn.close()


def write_ch(df: DataFrame, table: str) -> None:
    full_name = _table(table)
    exec_ch(df.sparkSession, f"TRUNCATE TABLE IF EXISTS {full_name}")
    (
        df.write.format("jdbc")
        .option("url", ch_jdbc_url())
        .option("dbtable", full_name)
        .option("user", ch_properties()["user"])
        .option("password", ch_properties()["password"])
        .option("driver", ch_properties()["driver"])
        .mode("append")
        .save()
    )
