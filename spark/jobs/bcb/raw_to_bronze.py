import sys
from pyspark.sql import functions as F

from utils.spark_session import create_spark_session
from config.bcb_series import BCB_SERIES
from config.storage import RAW_BUCKET, BRONZE_BUCKET


def transform_series(
    spark,
    series_name: str,
    ingestion_date: str
):

    raw_path = (
        f"s3a://{RAW_BUCKET}/"
        f"{series_name}/"
        f"ingestion_date={ingestion_date}/"
        f"response.json"
    )

    bronze_path = (
        f"s3a://{BRONZE_BUCKET}/"
        f"{series_name}"
    )

    print("")
    print("=" * 60)
    print(f"Start Bronze transformation: {series_name}")
    print(f"RAW path: {raw_path}")
    print("=" * 60)

    df = (
        spark.read
        .option("multiLine", "true")
        .json(raw_path)
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
            "ingestion_date",
            F.lit(ingestion_date)
        )
    )

    df = (
        df
        .withColumn(
            "year",
            F.year("data")
        )
        .withColumn(
            "month",
            F.month("data")
        )
    )

    print("Bronze schema:")
    df.printSchema()

    print("Rows:", df.count())

    (
        df.write
        .mode("append")
        .partitionBy(
            "year",
            "month"
        )
        .parquet(bronze_path)
    )

    print(
        f"Bronze saved: s3://{BRONZE_BUCKET}/{series_name}"
    )


def main():

    if len(sys.argv) != 2:
        raise ValueError(
            "Usage: bcb_raw_to_bronze.py <ingestion_date>"
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
            "Bronze transformation failed for: "
            + ", ".join(failed_series)
        )

    print("")
    print("RAW to Bronze completed successfully.")


if __name__ == "__main__":
    main()