import os

from utils.spark_session import create_spark_session
from config.storage import GOLD_BUCKET


def load_gold_to_postgres(
    spark,
    table_name: str
):

    gold_path = (
        f"s3a://{GOLD_BUCKET}/"
        f"{table_name}"
    )

    print("")
    print("=" * 60)
    print(f"Loading Gold -> PostgreSQL: {table_name}")
    print(f"Gold path: {gold_path}")
    print("=" * 60)

    df = spark.read.parquet(gold_path)

    print("")
    print("Gold schema:")
    df.printSchema()

    print("")
    print("Rows:", df.count())

    postgres_host = os.getenv("POSTGRES_HOST")
    postgres_port = os.getenv("POSTGRES_PORT")
    postgres_db = os.getenv("POSTGRES_DB")
    postgres_user = os.getenv("POSTGRES_USER")
    postgres_password = os.getenv("POSTGRES_PASSWORD")

    postgres_config = {
        "POSTGRES_HOST": postgres_host,
        "POSTGRES_PORT": postgres_port,
        "POSTGRES_DB": postgres_db,
        "POSTGRES_USER": postgres_user,
        "POSTGRES_PASSWORD": postgres_password,
    }

    missing_config = [
        key
        for key, value in postgres_config.items()
        if not value
    ]

    if missing_config:
        raise RuntimeError(
            "Missing PostgreSQL configuration: "
            + ", ".join(missing_config)
        )

    jdbc_url = (
        f"jdbc:postgresql://"
        f"{postgres_host}:{postgres_port}/"
        f"{postgres_db}"
    )

    properties = {
        "user": postgres_user,
        "password": postgres_password,
        "driver": "org.postgresql.Driver",
    }

    (
        df.write
        .mode("overwrite")
        .jdbc(
            url=jdbc_url,
            table=f"gold.{table_name}",
            properties=properties
        )
    )

    print("")
    print(
        f"PostgreSQL table created: "
        f"gold.{table_name}"
    )


def main():

    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    load_gold_to_postgres(
        spark=spark,
        table_name="macro_monthly"
    )

    spark.stop()

    print("")
    print("Gold -> PostgreSQL completed successfully.")


if __name__ == "__main__":
    main()