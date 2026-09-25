import io
import json

import boto3

from config.storage import (
    MINIO_ENDPOINT,
    MINIO_ROOT_USER,
    MINIO_ROOT_PASSWORD,
    BRONZE_BUCKET,
)

STAGING_PREFIX = "_staging"
CONTROL_PREFIX = "_control"


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


def save_raw_staging(
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
        f"{STAGING_PREFIX}/"
        f"ingestion_date={ingestion_date}/"
        f"batch_id={batch_id}/"
        f"{series_name}/"
        f"response.json"
    )

    client.upload_fileobj(
        buffer,
        BRONZE_BUCKET,
        key,
    )

    print(
        f"STAGING saved: "
        f"s3://{BRONZE_BUCKET}/{key}"
    )


def copy_previous_batch(
    series_name: str,
    previous_ingestion_date: str,
    previous_batch_id: str,
    ingestion_date: str,
    batch_id: str,
):

    client = get_minio_client()

    source_key = (
        f"{series_name}/"
        f"ingestion_date="
        f"{previous_ingestion_date}/"
        f"batch_id="
        f"{previous_batch_id}/"
        f"response.json"
    )

    staging_key = (
        f"{STAGING_PREFIX}/"
        f"ingestion_date={ingestion_date}/"
        f"batch_id={batch_id}/"
        f"{series_name}/"
        f"response.json"
    )

    try:

        client.copy_object(
            Bucket=BRONZE_BUCKET,
            CopySource={
                "Bucket": BRONZE_BUCKET,
                "Key": source_key,
            },
            Key=staging_key,
        )

    except Exception as error:

        raise RuntimeError(
            f"Failed to copy previous batch "
            f"for series: {series_name}"
        ) from error

    print(
        f"Previous batch reused: "
        f"{series_name}"
    )


def commit_batch(
    ingestion_date: str,
    batch_id: str,
    series_status: dict,
):

    client = get_minio_client()

    staging_prefix = (
        f"{STAGING_PREFIX}/"
        f"ingestion_date={ingestion_date}/"
        f"batch_id={batch_id}/"
    )

    response = client.list_objects_v2(
        Bucket=BRONZE_BUCKET,
        Prefix=staging_prefix,
    )

    objects = response.get(
        "Contents",
        [],
    )

    if not objects:

        raise RuntimeError(
            "Cannot commit empty batch."
        )

    print(
        f"Publishing {len(objects)} "
        f"staging object(s)..."
    )

    for obj in objects:

        staging_key = obj["Key"]

        relative_key = staging_key[
            len(staging_prefix):
        ]

        series_name = relative_key.split(
            "/"
        )[0]

        bronze_key = (
            f"{series_name}/"
            f"ingestion_date={ingestion_date}/"
            f"batch_id={batch_id}/"
            f"response.json"
        )

        client.copy_object(
            Bucket=BRONZE_BUCKET,
            CopySource={
                "Bucket": BRONZE_BUCKET,
                "Key": staging_key,
            },
            Key=bronze_key,
        )

        print(
            f"Bronze published: "
            f"s3://{BRONZE_BUCKET}/{bronze_key}"
        )

    commit_key = (
        f"{CONTROL_PREFIX}/"
        f"batches/"
        f"ingestion_date={ingestion_date}/"
        f"batch_id={batch_id}.json"
    )

    manifest = {
        "batch_id": batch_id,
        "ingestion_date": ingestion_date,
        "series": series_status,
        "status": "committed",
    }

    body = json.dumps(
        manifest,
        ensure_ascii=False,
        indent=2,
    ).encode("utf-8")

    client.put_object(
        Bucket=BRONZE_BUCKET,
        Key=commit_key,
        Body=body,
        ContentType="application/json",
    )

    print(
        f"Batch committed: "
        f"s3://{BRONZE_BUCKET}/{commit_key}"
    )


def delete_staging_batch(
    ingestion_date: str,
    batch_id: str,
):

    client = get_minio_client()

    prefix = (
        f"{STAGING_PREFIX}/"
        f"ingestion_date={ingestion_date}/"
        f"batch_id={batch_id}/"
    )

    response = client.list_objects_v2(
        Bucket=BRONZE_BUCKET,
        Prefix=prefix,
    )

    objects = response.get(
        "Contents",
        [],
    )

    if not objects:

        print(
            "No staging objects to delete."
        )

        return

    client.delete_objects(
        Bucket=BRONZE_BUCKET,
        Delete={
            "Objects": [
                {
                    "Key": obj["Key"]
                }
                for obj in objects
            ]
        },
    )

    print(
        f"Staging batch deleted: "
        f"{batch_id}"
    )

def commit_series(
    series_name: str,
    ingestion_date: str,
    batch_id: str,
):
    client = get_minio_client()

    staging_key = (
        f"{STAGING_PREFIX}/"
        f"ingestion_date={ingestion_date}/"
        f"batch_id={batch_id}/"
        f"{series_name}/"
        f"response.json"
    )

    bronze_key = (
        f"{series_name}/"
        f"ingestion_date={ingestion_date}/"
        f"batch_id={batch_id}/"
        f"response.json"
    )

    try:

        client.copy_object(
            Bucket=BRONZE_BUCKET,
            CopySource={
                "Bucket": BRONZE_BUCKET,
                "Key": staging_key,
            },
            Key=bronze_key,
        )

    except Exception as error:

        raise RuntimeError(
            f"Failed to publish Bronze data "
            f"for series: {series_name}"
        ) from error

    print(
        f"Bronze published: "
        f"s3://{BRONZE_BUCKET}/{bronze_key}"
    )

def write_batch_manifest(
    ingestion_date: str,
    batch_id: str,
    series_status: dict,
    status: str,
):
    client = get_minio_client()

    commit_key = (
        f"{CONTROL_PREFIX}/"
        f"batches/"
        f"ingestion_date={ingestion_date}/"
        f"batch_id={batch_id}.json"
    )

    manifest = {
        "batch_id": batch_id,
        "ingestion_date": ingestion_date,
        "series": series_status,
        "status": status,
    }

    body = json.dumps(
        manifest,
        ensure_ascii=False,
        indent=2,
    ).encode("utf-8")

    client.put_object(
        Bucket=BRONZE_BUCKET,
        Key=commit_key,
        Body=body,
        ContentType="application/json",
    )

    print(
        f"Batch manifest written: "
        f"s3://{BRONZE_BUCKET}/{commit_key}"
    )

def delete_staging_series(
    series_name: str,
    ingestion_date: str,
    batch_id: str,
):

    client = get_minio_client()

    prefix = (
        f"{STAGING_PREFIX}/"
        f"ingestion_date={ingestion_date}/"
        f"batch_id={batch_id}/"
        f"{series_name}/"
    )

    response = client.list_objects_v2(
        Bucket=BRONZE_BUCKET,
        Prefix=prefix,
    )

    objects = response.get(
        "Contents",
        [],
    )

    if not objects:
        return

    client.delete_objects(
        Bucket=BRONZE_BUCKET,
        Delete={
            "Objects": [
                {"Key": obj["Key"]}
                for obj in objects
            ]
        },
    )

    print(
        f"Staging deleted: "
        f"{series_name}"
    )