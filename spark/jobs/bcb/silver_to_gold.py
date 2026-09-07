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

    # ---------------------------------------------------------
    # 1. CREATE REFERENCE MONTH
    # ---------------------------------------------------------

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

    # ---------------------------------------------------------
    # 2. AGGREGATE TO MONTHLY
    # ---------------------------------------------------------

    if frequency == "daily":

        # Daily series:
        # calculate monthly average

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

        # Monthly series:
        # keep the available monthly value

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

    return df


def build_gold(spark):

    series_dataframes = []

    # ---------------------------------------------------------
    # 3. PROCESS ALL SERIES
    # ---------------------------------------------------------

    for series_name, series_config in BCB_SERIES.items():

        df = prepare_series(
            spark=spark,
            series_name=series_name,
            frequency=series_config["frequency"]
        )

        series_dataframes.append(df)

    # ---------------------------------------------------------
    # 4. UNION ALL SERIES
    # ---------------------------------------------------------

    print("")
    print("Combining series...")

    combined_df = reduce(
        lambda left, right: left.unionByName(right),
        series_dataframes
    )

    print("")
    print("Long format:")

    combined_df.printSchema()

    print(
        "Rows before pivot:",
        combined_df.count()
    )

    # ---------------------------------------------------------
    # 5. PIVOT
    # ---------------------------------------------------------

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

    return gold_df


def update_gold(
    spark,
    new_gold_df
):

    gold_path = (
        f"s3a://{GOLD_BUCKET}/"
        f"macro_monthly"
    )

    print("")
    print("=" * 60)
    print("Updating Gold")
    print("=" * 60)

    # ---------------------------------------------------------
    # 6. CHECK IF GOLD EXISTS
    # ---------------------------------------------------------

    gold_exists = False

    try:

        spark.read.parquet(
            gold_path
        ).limit(1).count()

        gold_exists = True

    except Exception:

        gold_exists = False

    # ---------------------------------------------------------
    # 7. INITIAL LOAD
    # ---------------------------------------------------------

    if not gold_exists:

        print("Gold does not exist.")
        print("Performing initial load.")

        (
            new_gold_df.write
            .mode("overwrite")
            .parquet(gold_path)
        )

        print("Initial Gold load completed.")

        return

    # ---------------------------------------------------------
    # 8. READ EXISTING GOLD
    # ---------------------------------------------------------

    existing_gold_df = (
        spark.read
        .parquet(gold_path)
    )

    # ---------------------------------------------------------
    # 9. REMOVE MONTHS THAT WILL BE UPDATED
    # ---------------------------------------------------------

    existing_without_new = (
        existing_gold_df.alias("existing")
        .join(
            new_gold_df
            .select("reference_month")
            .distinct()
            .alias("new"),
            on=F.col(
                "existing.reference_month"
            ) == F.col(
                "new.reference_month"
            ),
            how="left_anti"
        )
    )

    # ---------------------------------------------------------
    # 10. UNION OLD + NEW
    # ---------------------------------------------------------

    final_gold_df = (
        existing_without_new
        .unionByName(
            new_gold_df,
            allowMissingColumns=True
        )
        .orderBy("reference_month")
    )

    # ---------------------------------------------------------
    # 11. WRITE GOLD
    # ---------------------------------------------------------

    (
        final_gold_df.write
        .mode("overwrite")
        .parquet(gold_path)
    )

    print(
        "Gold updated successfully."
    )

    print(
        "Gold rows:",
        final_gold_df.count()
    )


def main():

    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    print("")
    print("=" * 60)
    print("Starting Silver -> Gold transformation")
    print("=" * 60)

    # ---------------------------------------------------------
    # 12. BUILD NEW GOLD DATA
    # ---------------------------------------------------------

    new_gold_df = build_gold(spark)

    print("")
    print("New Gold schema:")

    new_gold_df.printSchema()

    print("")
    print(
        "New Gold rows:",
        new_gold_df.count()
    )

    # ---------------------------------------------------------
    # 13. UPDATE GOLD
    # ---------------------------------------------------------

    update_gold(
        spark=spark,
        new_gold_df=new_gold_df
    )

    spark.stop()

    print("")
    print("=" * 60)
    print("Silver -> Gold completed successfully.")
    print("=" * 60)


if __name__ == "__main__":
    main()