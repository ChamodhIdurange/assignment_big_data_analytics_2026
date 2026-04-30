"""Load Olist CSV files into Spark DataFrames."""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession


def read_csv(spark: SparkSession, path: Path, **options) -> DataFrame:
    """Read a CSV file into a DataFrame with sensible defaults for Olist-style data."""
    defaults = {
        "header": True,
        "inferSchema": True,
        "encoding": "UTF-8",
    }
    defaults.update(options)
    return spark.read.csv(str(path), **defaults)


def load_olist_tables(spark: SparkSession, raw_dir: Path) -> dict[str, DataFrame]:
    """Load all expected CSV files from raw_dir. Keys are table names without .csv."""
    raw_dir = Path(raw_dir)
    frames: dict[str, DataFrame] = {}
    for csv_path in sorted(raw_dir.glob("*.csv")):
        name = csv_path.stem
        frames[name] = read_csv(spark, csv_path)
    return frames
