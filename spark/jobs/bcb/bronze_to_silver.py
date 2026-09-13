import sys

from pyspark.sql.functions import (
    col,
    current_timestamp,
    month,
    to_date,
    year,
)

from config.bcb_series import BCB_SERIES
from utils.spark_session import create_spark_session


BRONZE_BUCKET = "bcb-bronze"
SILVER_BUCKET = "bcb-silver"


def path_exists(spark, path):

    hadoop_path = (
        spark._jvm.org.apache.hadoop.fs.Path(path)
    )

    fs = hadoop_path.getFileSystem(
        spark._jsc.hadoopConfiguration()
    )

    return fs.exists(hadoop_path)


def is_batch_committed(
    spark,
    ingestion_date,
    batch_id,
):

    commit_path = (
        f"s3a://{BRONZE_BUCKET}/"
        f"_control/"
        f"batches/"
        f"ingestion_date={ingestion_date}/"
        f"batch_id={batch_id}.json"
    )

    return path_exists(
        spark,
        commit_path,
    )

def process_series(
    spark,
    series_name,
    ingestion_date,
    batch_id,
):

    print("")
    print("=" * 60)
    print(f"Bronze -> Silver: {series_name}")
    print(f"Ingestion date: {ingestion_date}")
    print(f"Batch ID: {batch_id}")
    print("=" * 60)

    bronze_path = (
        f"s3a://{BRONZE_BUCKET}/"
        f"{series_name}/"
        f"ingestion_date={ingestion_date}/"
        f"batch_id={batch_id}/"
        f"response.json"
    )

    silver_path = (
        f"s3a://{SILVER_BUCKET}/"
        f"{series_name}"
    )

    print(
        f"Bronze path: {bronze_path}"
    )

    if not path_exists(
        spark,
        bronze_path,
    ):

        print(
            "Bronze batch not found."
        )

        print(
            "Skipping series."
        )

        return

    df = spark.read.json(
        bronze_path
    )

    if df.rdd.isEmpty():

        print(
            "Bronze batch is empty."
        )

        print(
            "Skipping series."
        )

        return

    print(
        f"Bronze rows: {df.count()}"
    )

    df = (
        df
        .withColumn(
            "data",
            to_date(
                col("data"),
                "dd/MM/yyyy",
            ),
        )
        .withColumn(
            "valor",
            col("valor").cast("double"),
        )
        .withColumn(
            "ingestion_timestamp",
            current_timestamp(),
        )
        .withColumn(
            "year",
            year(col("data")),
        )
        .withColumn(
            "month",
            month(col("data")),
        )
        .dropDuplicates(["data"])
    )

    if path_exists(
        spark,
        silver_path,
    ):

        print(
            "Silver already exists."
        )

        print(
            "Checking for existing reference dates..."
        )

        silver_df = (
            spark.read
            .parquet(silver_path)
            .select("data")
            .dropDuplicates(["data"])
        )

        df = df.join(
            silver_df,
            on="data",
            how="left_anti",
        )

    if df.rdd.isEmpty():

        print(
            "All Bronze records already exist "
            "in Silver."
        )

        print(
            "Skipping series."
        )

        return

    rows_to_append = df.count()

    print(
        f"Rows to append: {rows_to_append}"
    )

    (
        df.write
        .mode("append")
        .partitionBy("year", "month")
        .parquet(silver_path)
    )

    print(
        "Silver updated successfully."
    )


def main(
    ingestion_date,
    batch_id,
):

    spark = create_spark_session()

    if not is_batch_committed(
        spark,
        ingestion_date,
        batch_id,
    ):

        raise RuntimeError(
            "Bronze batch is not committed."
        )

    try:

        print("")
        print("=" * 60)
        print("Starting Bronze -> Silver")
        print(
            f"Ingestion date: "
            f"{ingestion_date}"
        )
        print(
            f"Batch ID: {batch_id}"
        )
        print("=" * 60)

        if not is_batch_committed(
            spark,
            ingestion_date,
            batch_id,
        ):

            raise RuntimeError(
                "Bronze batch is not committed. "
                "The ingestion batch is not valid."
            )

        failed_series = []

        for series_name in BCB_SERIES:

            try:

                process_series(
                    spark=spark,
                    series_name=series_name,
                    ingestion_date=ingestion_date,
                    batch_id=batch_id,
                )

            except Exception as error:

                print(
                    f"Error processing "
                    f"{series_name}: {error}"
                )

                failed_series.append(
                    series_name
                )

        if failed_series:

            raise RuntimeError(
                "Bronze -> Silver failed for: "
                + ", ".join(failed_series)
            )

        print("")
        print("=" * 60)
        print(
            "Bronze -> Silver "
            "completed successfully."
        )
        print("=" * 60)

    finally:

        spark.stop()


if __name__ == "__main__":

    if len(sys.argv) != 3:

        raise ValueError(
            "Usage: "
            "bronze_to_silver.py "
            "<ingestion_date> "
            "<batch_id>"
        )

    ingestion_date = sys.argv[1]

    batch_id = sys.argv[2]

    main(
        ingestion_date=ingestion_date,
        batch_id=batch_id,
    )