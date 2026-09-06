from functools import reduce

from pyspark.sql import functions as F

from config.bcb_series import BCB_SERIES
from config.storage import SILVER_BUCKET, GOLD_BUCKET
from utils.spark_session import create_spark_session


def read_series(spark, series_name):

    silver_path = (
        f"s3a://{SILVER_BUCKET}/{series_name}"
    )

    df = spark.read.parquet(silver_path)

    return df


def prepare_monthly_series(spark, series_name, frequency):

    df = read_series(
        spark,
        series_name
    )

    if frequency == "daily":

        df = (
            df
            .withColumn(
                "reference_month",
                F.date_trunc("month", F.col("data"))
            )
            .groupBy("reference_month")
            .agg(
                F.avg("valor").alias(series_name)
            )
        )

    else:

        df = (
            df
            .withColumn(
                "reference_month",
                F.date_trunc("month", F.col("data"))
            )
            .select(
                "reference_month",
                F.col("valor").alias(series_name)
            )
        )

    return df


def main():

    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    print("")
    print("=" * 60)
    print("Starting Silver to Gold transformation")
    print("=" * 60)

    monthly_dataframes = []

    for series_name, series_config in BCB_SERIES.items():

        print("")
        print(f"Processing: {series_name}")

        df = prepare_monthly_series(
            spark,
            series_name,
            series_config["frequency"]
        )

        monthly_dataframes.append(df)

    gold_df = reduce(
        lambda left, right: left.join(
            right,
            on="reference_month",
            how="outer"
        ),
        monthly_dataframes
    )

    gold_df = (
        gold_df
        .orderBy("reference_month")
    )

    print("")
    print("Gold schema:")
    gold_df.printSchema()

    print("Rows:", gold_df.count())

    gold_path = (
        f"s3a://{GOLD_BUCKET}/macro_monthly"
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
    print("Silver to Gold completed successfully.")


if __name__ == "__main__":
    main()