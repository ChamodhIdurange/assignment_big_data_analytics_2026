"""Clean and transform Olist DataFrames (nulls, dates, types)."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def drop_all_null_rows(df: DataFrame) -> DataFrame:
    """Drop rows where every column is null."""
    cond = F.lit(False)
    for c in df.columns:
        cond = cond | F.col(c).isNotNull()
    return df.filter(cond)


def parse_date_column(df: DataFrame, col_name: str, fmt: str = "yyyy-MM-dd HH:mm:ss") -> DataFrame:
    """Parse a string column to timestamp."""
    return df.withColumn(col_name, F.to_timestamp(F.col(col_name), fmt))
