import os

from pyspark.sql import SparkSession


def create_spark_session():

    minio_endpoint = os.getenv("MINIO_ENDPOINT")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY")

    return (
        SparkSession.builder
        .appName("BCB-Data-Pipeline")

        .config(
            "spark.hadoop.fs.s3a.endpoint",
            f"http://{minio_endpoint}"
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            minio_access_key
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            minio_secret_key
        )
        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "true"
        )
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )

        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )

        .config(
            "spark.log.level",
            "WARN"
        )

        .getOrCreate()
    )