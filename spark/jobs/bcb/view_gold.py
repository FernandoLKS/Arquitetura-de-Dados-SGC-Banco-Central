from utils.spark_session import create_spark_session
from config.storage import GOLD_BUCKET


def main():

    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    gold_path = (
        f"s3a://{GOLD_BUCKET}/macro_monthly"
    )

    print("")
    print("=" * 60)
    print("GOLD DATASET")
    print("=" * 60)

    df = spark.read.parquet(gold_path)

    print("")
    print("Schema:")
    df.printSchema()

    print("")
    print(f"Rows: {df.count()}")

    print("")
    print("Data:")
    df.show(
        1000,
        truncate=False
    )

    spark.stop()


if __name__ == "__main__":
    main()