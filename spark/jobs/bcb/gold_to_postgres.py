import os

from pyspark.sql import functions as F

from utils.spark_session import create_spark_session
from config.storage import GOLD_BUCKET


def get_postgres_config():

    config = {
        "host": os.getenv("POSTGRES_HOST"),
        "port": os.getenv("POSTGRES_PORT"),
        "db": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
    }

    missing_config = [
        key
        for key, value in config.items()
        if not value
    ]

    if missing_config:
        raise RuntimeError(
            "Missing PostgreSQL configuration: "
            + ", ".join(missing_config)
        )

    return config


def create_jdbc_config(postgres_config):

    jdbc_url = (
        f"jdbc:postgresql://"
        f"{postgres_config['host']}:"
        f"{postgres_config['port']}/"
        f"{postgres_config['db']}"
    )

    properties = {
        "user": postgres_config["user"],
        "password": postgres_config["password"],
        "driver": "org.postgresql.Driver",
    }

    return jdbc_url, properties


def get_last_postgres_month(
    spark,
    jdbc_url,
    properties,
    table_name,
):

    query = f"""
        (
            SELECT MAX(reference_month) AS last_month
            FROM {table_name}
        ) AS last_month_query
    """

    try:

        df = (
            spark.read
            .jdbc(
                url=jdbc_url,
                table=query,
                properties=properties,
            )
        )

        row = df.first()

        if row is None:
            return None

        return row["last_month"]

    except Exception as error:

        print(
            "Could not read last PostgreSQL month."
        )

        print(
            f"Reason: {error}"
        )

        print(
            "Assuming initial load."
        )

        return None


def load_gold_to_postgres(
    spark,
    table_name: str,
):

    gold_path = (
        f"s3a://{GOLD_BUCKET}/"
        f"{table_name}"
    )

    target_table = (
        f"gold.{table_name}"
    )

    print("")
    print("=" * 60)
    print(
        f"Loading Gold -> PostgreSQL: "
        f"{table_name}"
    )
    print(
        f"Gold path: {gold_path}"
    )
    print(
        f"Target table: {target_table}"
    )
    print("=" * 60)

    postgres_config = get_postgres_config()

    jdbc_url, properties = (
        create_jdbc_config(
            postgres_config
        )
    )

    print("")
    print("Reading Gold Delta table...")

    gold_df = (
        spark.read
        .format("delta")
        .load(gold_path)
    )

    print("")
    print("Gold schema:")
    gold_df.printSchema()

    last_postgres_month = (
        get_last_postgres_month(
            spark=spark,
            jdbc_url=jdbc_url,
            properties=properties,
            table_name=target_table,
        )
    )

    print("")
    print(
        f"Last PostgreSQL month: "
        f"{last_postgres_month}"
    )

    if last_postgres_month is None:

        print("")
        print(
            "No data found in PostgreSQL."
        )

        print(
            "Performing initial load..."
        )

        (
            gold_df
            .orderBy("reference_month")
            .write
            .mode("append")
            .jdbc(
                url=jdbc_url,
                table=target_table,
                properties=properties,
            )
        )

        print(
            "Initial PostgreSQL load completed."
        )

        return

    new_df = (
        gold_df
        .filter(
            F.col("reference_month")
            > F.lit(last_postgres_month)
        )
    )

    new_rows = (
        new_df
        .select("reference_month")
        .limit(1)
        .count()
    )

    if new_rows == 0:

        print("")
        print(
            "No new data to load "
            "into PostgreSQL."
        )

        return

    print("")
    print(
        "New data detected."
    )

    print(
        "Appending new data "
        "to PostgreSQL..."
    )

    (
        new_df
        .orderBy("reference_month")
        .write
        .mode("append")
        .jdbc(
            url=jdbc_url,
            table=target_table,
            properties=properties,
        )
    )

    print("")
    print(
        "PostgreSQL incremental "
        "load completed."
    )


def main():

    spark = create_spark_session()

    try:

        load_gold_to_postgres(
            spark=spark,
            table_name="macro_monthly",
        )

    finally:

        spark.stop()

    print("")
    print("=" * 60)
    print(
        "Gold -> PostgreSQL "
        "completed successfully."
    )
    print("=" * 60)


if __name__ == "__main__":

    main()