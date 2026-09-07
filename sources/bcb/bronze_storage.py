import io
import json

import boto3

from config.storage import (
    MINIO_ENDPOINT,
    MINIO_ROOT_USER,
    MINIO_ROOT_PASSWORD,
    BRONZE_BUCKET,
)


def get_minio_client():

    return boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ROOT_USER,
        aws_secret_access_key=MINIO_ROOT_PASSWORD,
    )


def create_bucket_if_not_exists():

    client = get_minio_client()

    buckets = client.list_buckets()["Buckets"]

    exists = any(
        bucket["Name"] == BRONZE_BUCKET
        for bucket in buckets
    )

    if not exists:

        client.create_bucket(
            Bucket=BRONZE_BUCKET
        )

        print(
            f"Bucket created: {BRONZE_BUCKET}"
        )


def save_raw(
    data,
    series_name: str,
    ingestion_date: str,
    batch_id: str,
):

    client = get_minio_client()

    create_bucket_if_not_exists()

    buffer = io.BytesIO()

    buffer.write(
        json.dumps(
            data,
            ensure_ascii=False,
        ).encode("utf-8")
    )

    buffer.seek(0)

    key = (
        f"{series_name}/"
        f"ingestion_date={ingestion_date}/"
        f"batch_id={batch_id}/"
        f"response.json"
    )

    client.upload_fileobj(
        buffer,
        BRONZE_BUCKET,
        key,
    )

    print(
        f"RAW saved: "
        f"s3://{BRONZE_BUCKET}/{key}"
    )