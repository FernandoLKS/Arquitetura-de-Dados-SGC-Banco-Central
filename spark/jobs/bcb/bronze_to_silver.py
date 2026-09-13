import json
import sys

from delta.tables import DeltaTable

from pyspark.sql.functions import (
    col,
    current_timestamp,
    month,
    to_date,
    year,
)

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


def read_batch_manifest(
    spark,
    ingestion_date,
    batch_id,
):
    manifest_path = (
        f"s3a://{BRONZE_BUCKET}/"
        f"_control/"
        f"batches/"
        f"ingestion_date={ingestion_date}/"
        f"batch_id={batch_id}.json"
    )

    print(
        f"Reading batch manifest: "
        f"{manifest_path}"
    )

    hadoop_path = (
        spark._jvm.org.apache.hadoop.fs.Path(
            manifest_path
        )
    )

    fs = hadoop_path.getFileSystem(
        spark._jsc.hadoopConfiguration()
    )

    input_stream = fs.open(hadoop_path)

    try:
        content = (
            spark._jvm.org.apache.commons.io.IOUtils
            .toString(
                input_stream,
                "UTF-8",
            )
        )
    finally:
        input_stream.close()

    print(
        f"Manifest content length: "
        f"{len(content)}"
    )

    print(
        f"Manifest content: "
        f"{content}"
    )

    return json.loads(content)

def get_changed_series(
    manifest,
):

    series_status = manifest.get(
        "series",
        {},
    )

    return [
        series_name
        for series_name, status in series_status.items()
        if status == "new_data"
    ]


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

    rows_to_merge = df.count()

    print(
        f"Rows to merge: {rows_to_merge}"
    )

    # Primeira execução:
    # cria a tabela Delta
    if not path_exists(
        spark,
        silver_path,
    ):

        print(
            "Silver Delta table does not exist."
        )

        print(
            "Creating Delta table..."
        )

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy(
                "year",
                "month",
            )
            .save(silver_path)
        )

        print(
            "Silver Delta table "
            "created successfully."
        )

        return

    # Tabela já existe:
    # executa MERGE
    print(
        "Silver Delta table already exists."
    )

    print(
        "Executing MERGE..."
    )

    delta_table = DeltaTable.forPath(
        spark,
        silver_path,
    )

    (
        delta_table.alias("target")
        .merge(
            df.alias("source"),
            "target.data = source.data",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(
        "Silver Delta table "
        "updated successfully."
    )


def main(
    ingestion_date,
    batch_id,
):

    spark = create_spark_session()

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

        # --------------------------------------------------
        # 1. Verifica se o batch foi realmente publicado
        # --------------------------------------------------

        if not is_batch_committed(
            spark,
            ingestion_date,
            batch_id,
        ):

            raise RuntimeError(
                "Bronze batch is not committed. "
                "The ingestion batch is not valid."
            )

        # --------------------------------------------------
        # 2. Lê o manifesto do batch
        # --------------------------------------------------

        manifest = read_batch_manifest(
            spark,
            ingestion_date,
            batch_id,
        )

        # --------------------------------------------------
        # 3. Identifica somente as séries alteradas
        # --------------------------------------------------

        changed_series = get_changed_series(
            manifest
        )

        print("")
        print(
            f"Series with new data: "
            f"{len(changed_series)}"
        )

        for series_name in changed_series:

            print(
                f"  - {series_name}"
            )

        # --------------------------------------------------
        # 4. Nada mudou
        # --------------------------------------------------

        if not changed_series:

            print("")
            print(
                "No series with new data."
            )

            print(
                "Nothing to process."
            )

            return

        # --------------------------------------------------
        # 5. Processa somente séries alteradas
        # --------------------------------------------------

        failed_series = []

        for series_name in changed_series:

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
                + ", ".join(
                    failed_series
                )
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