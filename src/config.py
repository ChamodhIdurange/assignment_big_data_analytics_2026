"""SparkSession setup and project file paths."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"
OUTPUT_DATA = PROJECT_ROOT / "outputs" / "data"
OUTPUT_VIZ = PROJECT_ROOT / "outputs" / "visualizations"


def get_spark(app_name: str = "olist_pyspark_project"):
    """Create and return a SparkSession."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .getOrCreate()
    )
