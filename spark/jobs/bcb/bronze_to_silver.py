import sys
from pyspark.sql import functions as F

from config.bcb_series import BCB_SERIES
from config.storage import BRONZE_BUCKET, SILVER_BUCKET
from utils.spark_session import create_spark_session


def transform_series(spark, series_name: str):

    bronze_path = f"s3a://{BRONZE_BUCKET}/{series_name}"
    silver_path = f"s3a://{SILVER_BUCKET}/{series_name}"

    print("")
    print("=" * 60)
    print(f"Start Silver transformation: {series_name}")
    print(f"Bronze path: {bronze_path}")
    print("=" * 60)

    df = spark.read.parquet(bronze_path)

    df = (
        df
        .select(
            "data",
            "valor",
            "ingestion_timestamp",
            "ingestion_date"
        )
        .filter(F.col("data").isNotNull())
        .filter(F.col("valor").isNotNull())
        .dropDuplicates(["data"])
        .orderBy("data")
    )

    print("Silver schema:")
    df.printSchema()

    print("Rows:", df.count())

    (
        df.write
        .mode("overwrite")
        .parquet(silver_path)
    )

    print(f"Silver saved: s3://{SILVER_BUCKET}/{series_name}")


def main():

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    failed_series = []

    for series_name in BCB_SERIES:

        try:
            transform_series(
                spark,
                series_name
            )

        except Exception as error:

            print(
                f"Error processing {series_name}: {error}"
            )

            failed_series.append(series_name)

    spark.stop()

    if failed_series:

        raise RuntimeError(
            "Silver transformation failed for: "
            + ", ".join(failed_series)
        )

    print("")
    print("Bronze to Silver completed successfully.")


if __name__ == "__main__":
    main()