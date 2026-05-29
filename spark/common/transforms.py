from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    quarter,
    row_number,
    to_date,
    trim,
    when,
    year,
    month,
    dayofmonth,
)
from pyspark.sql.window import Window


def _as_column(column) -> Column:
    return column if isinstance(column, Column) else col(column)


def clean_text(column) -> Column:
    return trim(_as_column(column))


def first_by(df: DataFrame, partition_cols, order_col="row_id") -> DataFrame:
    window = Window.partitionBy(*partition_cols).orderBy(col(order_col))
    return df.withColumn("_rn", row_number().over(window)).filter(col("_rn") == 1).drop("_rn")


def parse_sale_date(column) -> Column:
    return to_date(clean_text(column), "M/d/yyyy")


def null_if_blank(column) -> Column:
    c = clean_text(column)
    return when((c == "") | c.isNull(), lit(None)).otherwise(c)
