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
    print(f"Silver path: {silver_path}")
    print("=" * 60)

    # ---------------------------------------------------------
    # 1. READ BRONZE
    # ---------------------------------------------------------

    df_new = (
        spark.read
        .option("multiLine", "true")
        .json(bronze_path)
    )

    if df_new.rdd.isEmpty():
        print("No data found in Bronze.")
        return

    # ---------------------------------------------------------
    # 2. STANDARDIZE DATA TYPES
    # ---------------------------------------------------------

    df_new = (
        df_new
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

    # ---------------------------------------------------------
    # 3. REMOVE DUPLICATES FROM CURRENT INGESTION
    # ---------------------------------------------------------

    df_new = (
        df_new
        .dropDuplicates(["data"])
    )

    print("New rows:", df_new.count())

    # ---------------------------------------------------------
    # 4. CHECK IF SILVER ALREADY EXISTS
    # ---------------------------------------------------------

    silver_exists = False

    try:
        spark.read.parquet(silver_path).limit(1).count()
        silver_exists = True
    except Exception:
        silver_exists = False

    # ---------------------------------------------------------
    # 5. FIRST LOAD
    # ---------------------------------------------------------

    if not silver_exists:

        print("Silver does not exist.")
        print("Performing initial load.")

        (
            df_new.write
            .mode("overwrite")
            .partitionBy(
                "year",
                "month"
            )
            .parquet(silver_path)
        )

        print("Initial Silver load completed.")

        return

    # ---------------------------------------------------------
    # 6. READ EXISTING SILVER
    # ---------------------------------------------------------

    df_existing = (
        spark.read
        .parquet(silver_path)
    )

    # ---------------------------------------------------------
    # 7. REMOVE RECORDS THAT WILL BE UPDATED
    # ---------------------------------------------------------

    existing_without_new = (
        df_existing.alias("existing")
        .join(
            df_new
            .select("data")
            .distinct()
            .alias("new"),
            on=F.col("existing.data") == F.col("new.data"),
            how="left_anti"
        )
    )

    # ---------------------------------------------------------
    # 8. MERGE EXISTING + NEW
    # ---------------------------------------------------------

    df_final = (
        existing_without_new
        .unionByName(
            df_new,
            allowMissingColumns=True
        )
    )

    # ---------------------------------------------------------
    # 9. WRITE SILVER
    # ---------------------------------------------------------

    (
        df_final.write
        .mode("overwrite")
        .partitionBy(
            "year",
            "month"
        )
        .parquet(silver_path)
    )

    print("Silver updated successfully.")
    print("Total rows:", df_final.count())


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
    print("=" * 60)
    print("Bronze -> Silver completed successfully.")
    print("=" * 60)


if __name__ == "__main__":
    main()