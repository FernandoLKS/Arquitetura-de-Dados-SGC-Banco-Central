from functools import reduce

from pyspark.sql import functions as F

from config.bcb_series import BCB_SERIES
from config.storage import SILVER_BUCKET, GOLD_BUCKET
from utils.spark_session import create_spark_session


def path_exists(
    spark,
    path,
):

    hadoop_path = (
        spark._jvm.org.apache.hadoop.fs.Path(path)
    )

    fs = hadoop_path.getFileSystem(
        spark._jsc.hadoopConfiguration()
    )

    return fs.exists(hadoop_path)


def read_series(
    spark,
    series_name,
    frequency,
    silver_path,
):

    print("")
    print("-" * 60)
    print(f"Processing: {series_name}")
    print(f"Frequency: {frequency}")
    print("-" * 60)


    if not path_exists(
        spark,
        silver_path,
    ):

        print(
            "Silver data not found."
        )

        print(
            "Skipping series."
        )

        return None


    df = (
        spark.read
        .parquet(
            silver_path
        )
    )

    if df.rdd.isEmpty():

        print(
            "Silver is empty."
        )

        print(
            "Skipping series."
        )

        return None

    print(
        f"Silver rows: {df.count()}"
    )


    df = (
        df
        .withColumn(
            "reference_month",
            F.date_trunc(
                "month",
                F.col("data"),
            ),
        )
    )


    df = (
        df
        .select(
            "reference_month",
            "valor",
        )
        .withColumn(
            "series_name",
            F.lit(series_name),
        )
    )

    if frequency == "daily":

        print(
            "Aggregation: daily -> monthly average"
        )

        df = (
            df
            .groupBy(
                "reference_month",
                "series_name",
            )
            .agg(
                F.avg(
                    "valor"
                ).alias("valor")
            )
        )


    elif frequency == "monthly":

        print(
            "Aggregation: monthly -> monthly value"
        )

        df = (
            df
            .groupBy(
                "reference_month",
                "series_name",
            )
            .agg(
                F.first(
                    "valor",
                    ignorenulls=True,
                ).alias("valor")
            )
        )

    else:

        raise ValueError(
            f"Frequency not supported: {frequency}"
        )

    return df


def build_gold(
    spark,
):

    series_dataframes = []

    for series_name, series_config in BCB_SERIES.items():

        silver_path = (
            f"s3a://{SILVER_BUCKET}/"
            f"{series_name}"
        )

        df = read_series(
            spark=spark,
            series_name=series_name,
            frequency=series_config["frequency"],
            silver_path=silver_path,
        )

        if df is not None:

            series_dataframes.append(
                df
            )


    if not series_dataframes:

        print(
            "No Silver data available."
        )

        return None


    combined_df = reduce(
        lambda left, right:
        left.unionByName(right),
        series_dataframes,
    )


    gold_df = (
        combined_df
        .groupBy(
            "reference_month",
        )
        .pivot(
            "series_name",
        )
        .agg(
            F.first(
                "valor",
                ignorenulls=True,
            )
        )
        .orderBy(
            "reference_month",
        )
    )

    return gold_df


def write_gold(
    gold_df,
    gold_path,
):

    if gold_df is None:

        print(
            "No data available for Gold."
        )

        return

    if gold_df.rdd.isEmpty():

        print(
            "Gold DataFrame is empty."
        )

        return


    rows = gold_df.count()

    print(
        f"Gold rows: {rows}"
    )

    print(
        "Overwriting Gold..."
    )

    (
        gold_df
        .write
        .mode("overwrite")
        .partitionBy(
            "reference_month"
        )
        .parquet(
            gold_path
        )
    )

    print(
        "Gold overwrite completed successfully."
    )


def main():

    spark = create_spark_session()

    gold_path = (
        f"s3a://{GOLD_BUCKET}/"
        f"macro_monthly"
    )

    print("")
    print("=" * 60)
    print("Starting Silver -> Gold")
    print("=" * 60)


    gold_df = build_gold(
        spark=spark,
    )

    write_gold(
        gold_df=gold_df,
        gold_path=gold_path,
    )

    spark.stop()

    print("")
    print("=" * 60)
    print("Silver -> Gold completed successfully.")
    print("=" * 60)


if __name__ == "__main__":
    main()
