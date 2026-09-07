from functools import reduce

from pyspark.sql import functions as F

from config.bcb_series import BCB_SERIES
from config.storage import SILVER_BUCKET, GOLD_BUCKET
from utils.spark_session import create_spark_session


def read_series(spark, series_name):
    silver_path = (
        f"s3a://{SILVER_BUCKET}/"
        f"{series_name}"
    )

    return spark.read.parquet(silver_path)


def prepare_series(
    spark,
    series_name,
    frequency
):

    df = read_series(
        spark,
        series_name
    )

    print(f"Processing: {series_name}")

    df = (
        df
        .withColumn(
            "reference_month",
            F.date_trunc(
                "month",
                F.col("data")
            )
        )
        .select(
            "reference_month",
            "valor"
        )
        .withColumn(
            "series_name",
            F.lit(series_name)
        )
    )

    # Guarantee one observation per month.
    # Daily series use monthly average.
    # Monthly series use the available monthly value.
    if frequency == "daily":

        df = (
            df
            .groupBy(
                "reference_month",
                "series_name"
            )
            .agg(
                F.avg("valor").alias("valor")
            )
        )

    else:

        df = (
            df
            .groupBy(
                "reference_month",
                "series_name"
            )
            .agg(
                F.first("valor", ignorenulls=True).alias("valor")
            )
        )

    return df


def main():

    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    print("")
    print("=" * 60)
    print("Starting Silver -> Gold transformation")
    print("=" * 60)

    series_dataframes = []

    for series_name, series_config in BCB_SERIES.items():

        df = prepare_series(
            spark=spark,
            series_name=series_name,
            frequency=series_config["frequency"]
        )

        series_dataframes.append(df)

    print("")
    print("Combining series...")

    combined_df = reduce(
        lambda left, right: left.unionByName(right),
        series_dataframes
    )

    print("")
    print("Long format:")
    combined_df.printSchema()

    print("")
    print("Rows before pivot:", combined_df.count())

    print("")
    print("Creating monthly Gold table...")

    gold_df = (
        combined_df
        .groupBy("reference_month")
        .pivot("series_name")
        .agg(
            F.first(
                "valor",
                ignorenulls=True
            )
        )
        .orderBy("reference_month")
    )

    print("")
    print("Gold schema:")
    gold_df.printSchema()

    print("")
    print("Gold rows:", gold_df.count())

    gold_path = (
        f"s3a://{GOLD_BUCKET}/"
        f"macro_monthly"
    )

    (
        gold_df.write
        .mode("overwrite")
        .parquet(gold_path)
    )

    print("")
    print(
        f"Gold saved: "
        f"s3://{GOLD_BUCKET}/macro_monthly"
    )

    spark.stop()

    print("")
    print("Silver -> Gold completed successfully.")


if __name__ == "__main__":
    main()