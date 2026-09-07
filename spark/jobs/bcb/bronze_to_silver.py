import sys

from pyspark.sql import functions as F

from utils.spark_session import create_spark_session
from config.bcb_series import BCB_SERIES
from config.storage import BRONZE_BUCKET, SILVER_BUCKET


def transform_series(
    spark,
    series_name: str,
    ingestion_date: str
):

    bronze_path = (
        f"s3a://{BRONZE_BUCKET}/"
        f"{series_name}/"
        f"ingestion_date={ingestion_date}/"
        f"response.json"
    )

    silver_path = (
        f"s3a://{SILVER_BUCKET}/"
        f"{series_name}"
    )

    print("")
    print("=" * 60)
    print(f"Start Bronze -> Silver: {series_name}")
    print(f"Bronze path: {bronze_path}")
    print("=" * 60)

    df = (
        spark.read
        .option("multiLine", "true")
        .json(bronze_path)
    )

    df = (
        df
        .withColumn(
            "data",
            F.to_date(
                F.col("data"),
                "dd/MM/yyyy"
            )
        )
        .withColumn(
            "valor",
            F.col("valor").cast("double")
        )
        .withColumn(
            "ingestion_timestamp",
            F.current_timestamp()
        )
        .withColumn(
            "year",
            F.year("data")
        )
        .withColumn(
            "month",
            F.month("data")
        )
    )

    print("Silver schema:")
    df.printSchema()

    print("Rows:", df.count())

    (
        df.write
        .mode("overwrite")
        .partitionBy(
            "year",
            "month"
        )
        .parquet(silver_path)
    )

    print(
        f"Silver saved: s3://{SILVER_BUCKET}/{series_name}"
    )


def main():

    if len(sys.argv) != 2:
        raise ValueError(
            "Usage: bronze_to_silver.py <ingestion_date>"
        )

    ingestion_date = sys.argv[1]

    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    failed_series = []

    for series_name in BCB_SERIES:

        try:

            transform_series(
                spark=spark,
                series_name=series_name,
                ingestion_date=ingestion_date
            )

        except Exception as error:

            print(
                f"Error processing {series_name}: {error}"
            )

            failed_series.append(series_name)

    spark.stop()

    if failed_series:

        raise RuntimeError(
            "Bronze -> Silver transformation failed for: "
            + ", ".join(failed_series)
        )

    print("")
    print("Bronze -> Silver completed successfully.")


if __name__ == "__main__":
    main()