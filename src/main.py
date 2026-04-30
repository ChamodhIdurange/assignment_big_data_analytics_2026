"""Entry point: load, preprocess, analyze, and write outputs."""

from config import DATA_RAW, OUTPUT_DATA, OUTPUT_VIZ, get_spark
from loader import load_olist_tables


def main() -> None:
    spark = get_spark()
    try:
        OUTPUT_DATA.mkdir(parents=True, exist_ok=True)
        OUTPUT_VIZ.mkdir(parents=True, exist_ok=True)

        if not DATA_RAW.exists():
            print(f"Place Olist CSV files in: {DATA_RAW}")
            return

        tables = load_olist_tables(spark, DATA_RAW)
        print(f"Loaded {len(tables)} table(s): {', '.join(sorted(tables))}")
        # Import preprocess/analytics and wire your pipeline here.
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
