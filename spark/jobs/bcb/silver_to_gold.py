from functools import reduce

from pyspark.sql import functions as F

from config.bcb_series import BCB_SERIES
from config.storage import SILVER_BUCKET, GOLD_BUCKET
from utils.spark_session import create_spark_session


def path_exists(spark, path):
    hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    fs = hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())
    return fs.exists(hadoop_path)


def read_series(spark, series_name, frequency, silver_path):

    print(f"Reading Silver: {series_name}")

    if not path_exists(spark, silver_path):
        print(f"Silver path not found: {silver_path}")
        return None

    # Silver agora é Delta
    df = spark.read.format("delta").load(silver_path)

    if df.rdd.isEmpty():
        print(f"Silver is empty: {series_name}")
        return None

    df = (
        df
        .withColumn(
            "reference_month",
            F.date_trunc("month", F.col("data"))
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

    elif frequency == "monthly":

        df = (
            df
            .groupBy(
                "reference_month",
                "series_name"
            )
            .agg(
                F.first(
                    "valor",
                    ignorenulls=True
                ).alias("valor")
            )
        )

    else:
        raise ValueError(
            f"Unsupported frequency: {frequency}"
        )

    return df


def build_gold(spark):

    series_dataframes = []

    for series_name, series_config in BCB_SERIES.items():

        silver_path = (
            f"s3a://{SILVER_BUCKET}/{series_name}"
        )

        df = read_series(
            spark=spark,
            series_name=series_name,
            frequency=series_config["frequency"],
            silver_path=silver_path,
        )

        if df is not None:
            series_dataframes.append(df)

    if not series_dataframes:
        return None

    combined_df = reduce(
        lambda left, right: left.unionByName(right),
        series_dataframes
    )

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

    return gold_df


def write_gold(gold_df, gold_path):

    if gold_df is None:
        print("No data to write.")
        return

    print(f"Writing Gold Delta table: {gold_path}")

    (
        gold_df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("reference_month")
        .save(gold_path)
    )

    print("Gold Delta table written successfully.")


def main():

    spark = create_spark_session()

    try:

        print("=" * 60)
        print("Starting Silver -> Gold")
        print("=" * 60)

        gold_path = (
            f"s3a://{GOLD_BUCKET}/macro_monthly"
        )

        gold_df = build_gold(spark)

        if gold_df is None:
            print("No data available to build Gold.")
            return

        print("Gold schema:")
        gold_df.printSchema()

        print("Gold preview:")
        gold_df.show(10, truncate=False)

        write_gold(
            gold_df=gold_df,
            gold_path=gold_path,
        )

        print("=" * 60)
        print("Silver -> Gold completed successfully.")
        print("=" * 60)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()