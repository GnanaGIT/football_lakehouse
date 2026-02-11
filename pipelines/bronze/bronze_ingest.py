import argparse
import os
import sys
from pyspark.sql import SparkSession


def get_spark():
    return SparkSession.builder.appName("bronze_ingest").getOrCreate()


def parse_args():
    parser = argparse.ArgumentParser(description="Bronze CSV ingestion")

    parser.add_argument(
        "--raw-path",
        required=True,
        help="Folder containing raw CSV files"
    )

    parser.add_argument(
        "--bronze-path",
        required=True,
        help="Bronze output root folder"
    )

    return parser.parse_args()


def ingest_csvs(spark, raw_path, bronze_path):
    files = [f for f in os.listdir(raw_path) if f.endswith(".csv")]

    if not files:
        raise RuntimeError("No CSV files found in raw path")

    row_manifest = {}

    for file in files:
        table = file.replace(".csv", "")
        input_path = f"{raw_path}/{file}"
        output_path = f"{bronze_path}/{table}"

        print(f"[BRONZE] Ingesting {table}")

        df = spark.read.option("header", True).csv(input_path)

        count = df.count()

        if count == 0:
            raise RuntimeError(f"{table} produced zero rows")

        df.write.mode("overwrite").parquet(output_path)

        row_manifest[table] = count

        print(f"[BRONZE] {table}: {count} rows")

    return row_manifest


def main():
    args = parse_args()

    raw_path = args.raw_path
    bronze_path = args.bronze_path

    print(f"[START] Raw path: {raw_path}")
    print(f"[START] Bronze path: {bronze_path}")

    spark = get_spark()

    manifest = ingest_csvs(spark, raw_path, bronze_path)

    print("\n[MANIFEST]")
    for k, v in manifest.items():
        print(f"{k}: {v}")

    spark.stop()
    print("\n[SUCCESS] Bronze ingestion complete")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[FAILED] {str(e)}")
        sys.exit(1)
