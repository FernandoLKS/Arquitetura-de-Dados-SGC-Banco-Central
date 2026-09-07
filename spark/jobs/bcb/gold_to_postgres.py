import os

from utils.spark_session import create_spark_session
from config.storage import GOLD_BUCKET


def get_postgres_config():

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

    return {
        "host": postgres_host,
        "port": postgres_port,
        "db": postgres_db,
        "user": postgres_user,
        "password": postgres_password,
    }


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

    # ---------------------------------------------------------
    # 1. READ GOLD FROM MINIO
    # ---------------------------------------------------------

    df = spark.read.parquet(gold_path)

    print("")
    print("Gold schema:")
    df.printSchema()

    print("")
    print("Rows:", df.count())

    # ---------------------------------------------------------
    # 2. POSTGRES CONFIGURATION
    # ---------------------------------------------------------

    postgres_config = get_postgres_config()

    jdbc_url, properties = create_jdbc_config(
        postgres_config
    )

    target_table = f"gold.{table_name}"

    # Temporary table used only during the load
    staging_table = f"gold.{table_name}_staging"

    # ---------------------------------------------------------
    # 3. WRITE TO TEMPORARY STAGING TABLE
    # ---------------------------------------------------------

    print("")
    print("Writing data to PostgreSQL staging table...")

    (
        df.write
        .mode("overwrite")
        .jdbc(
            url=jdbc_url,
            table=staging_table,
            properties=properties
        )
    )

    print(
        f"Staging table created: {staging_table}"
    )

    # ---------------------------------------------------------
    # 4. CREATE TARGET TABLE IF NECESSARY
    # ---------------------------------------------------------

    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {target_table}
        AS
        SELECT *
        FROM {staging_table}
        WHERE 1 = 0;
    """

    # ---------------------------------------------------------
    # 5. UPSERT INTO TARGET TABLE
    # ---------------------------------------------------------

    # Columns from Gold
    columns = df.columns

    columns_sql = ", ".join(
        f'"{column}"'
        for column in columns
    )

    update_columns = [
        column
        for column in columns
        if column != "reference_month"
    ]

    update_sql = ", ".join(
        f'"{column}" = EXCLUDED."{column}"'
        for column in update_columns
    )

    upsert_sql = f"""
        INSERT INTO {target_table} (
            {columns_sql}
        )
        SELECT
            {columns_sql}
        FROM {staging_table}
        ON CONFLICT ("reference_month")
        DO UPDATE SET
            {update_sql};
    """

    # ---------------------------------------------------------
    # 6. EXECUTE SQL
    # ---------------------------------------------------------

    print("")
    print("Executing PostgreSQL upsert...")

    # Spark does not provide a convenient generic JDBC
    # connection for arbitrary SQL, so use the PostgreSQL
    # JDBC driver directly through the JVM.

    jvm = spark._sc._gateway.jvm

    connection = jvm.java.sql.DriverManager.getConnection(
        jdbc_url,
        postgres_config["user"],
        postgres_config["password"]
    )

    statement = connection.createStatement()

    try:

        statement.executeUpdate(
            create_table_sql
        )

        statement.executeUpdate(
            upsert_sql
        )

        print(
            "Upsert completed successfully."
        )

    finally:

        statement.close()
        connection.close()

    # ---------------------------------------------------------
    # 7. CLEAN STAGING TABLE
    # ---------------------------------------------------------

    print("")
    print("Removing staging table...")

    connection = jvm.java.sql.DriverManager.getConnection(
        jdbc_url,
        postgres_config["user"],
        postgres_config["password"]
    )

    statement = connection.createStatement()

    try:

        statement.executeUpdate(
            f"DROP TABLE IF EXISTS {staging_table}"
        )

    finally:

        statement.close()
        connection.close()

    print(
        f"PostgreSQL table updated: "
        f"{target_table}"
    )


def main():

    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    try:

        load_gold_to_postgres(
            spark=spark,
            table_name="macro_monthly"
        )

    finally:

        spark.stop()

    print("")
    print(
        "Gold -> PostgreSQL completed successfully."
    )


if __name__ == "__main__":
    main()