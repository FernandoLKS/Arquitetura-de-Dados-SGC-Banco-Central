import io
import json

import boto3
import pandas as pd

from sources.bcb.ingestion_config import (
    MINIO_ENDPOINT,
    MINIO_ROOT_USER,
    MINIO_ROOT_PASSWORD,
    MINIO_BUCKET
)


def get_minio_client():

    return boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ROOT_USER,
        aws_secret_access_key=MINIO_ROOT_PASSWORD
    )


def create_bucket_if_not_exists():

    client = get_minio_client()

    buckets = client.list_buckets()["Buckets"]

    exists = any(
        bucket["Name"] == MINIO_BUCKET
        for bucket in buckets
    )

    if not exists:

        client.create_bucket(
            Bucket=MINIO_BUCKET
        )

        print(f"Bucket create: {MINIO_BUCKET}")


def save_raw(
    data,
    series_name: str
):

    client = get_minio_client()

    create_bucket_if_not_exists()

    buffer = io.BytesIO()

    buffer.write(
        json.dumps(
            data,
            ensure_ascii=False
        ).encode("utf-8")
    )

    buffer.seek(0)

    key = (
        f"bcb/raw/"
        f"{series_name}/"
        f"response.json"
    )

    client.upload_fileobj(
        buffer,
        MINIO_BUCKET,
        key
    )

    print(
        f"RAW saved: "
        f"s3://{MINIO_BUCKET}/{key}"
    )


def save_parquet(
    df: pd.DataFrame,
    series_name: str
):

    client = get_minio_client()

    create_bucket_if_not_exists()

    df = df.copy()

    df["data"] = pd.to_datetime(df["data"])

    for year, df_year in df.groupby(
        df["data"].dt.year
    ):

        for month, df_month in df_year.groupby(
            df_year["data"].dt.month
        ):

            buffer = io.BytesIO()

            df_month.to_parquet(
                buffer,
                index=False
            )

            buffer.seek(0)

            key = (
                f"bcb/bronze/"
                f"{series_name}/"
                f"year={year}/"
                f"month={month:02d}/"
                f"data.parquet"
            )

            client.upload_fileobj(
                buffer,
                MINIO_BUCKET,
                key
            )

            print(
                f"Bronze saved: "
                f"s3://{MINIO_BUCKET}/{key}"
            )