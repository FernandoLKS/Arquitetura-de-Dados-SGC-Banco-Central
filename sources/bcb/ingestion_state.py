import json

from botocore.exceptions import ClientError

from .bronze_storage import get_minio_client
from config.storage import BRONZE_BUCKET


CONTROL_PREFIX = "_control"


def get_state_key(series_name: str) -> str:

    return (
        f"{CONTROL_PREFIX}/"
        f"{series_name}.json"
    )


def get_last_reference_date(
    series_name: str
):

    client = get_minio_client()

    try:

        response = client.get_object(
            Bucket=BRONZE_BUCKET,
            Key=get_state_key(series_name),
        )

        state = json.loads(
            response["Body"]
            .read()
            .decode("utf-8")
        )

        return state.get(
            "last_reference_date"
        )

    except ClientError as error:

        error_code = (
            error.response["Error"]["Code"]
        )

        if error_code == "NoSuchKey":
            return None

        raise


def update_state(
    series_name: str,
    last_reference_date: str,
    ingestion_date: str,
    rows_ingested: int,
):

    client = get_minio_client()

    state = {
        "series_name": series_name,
        "last_reference_date": last_reference_date,
        "last_ingestion_date": ingestion_date,
        "rows_ingested": rows_ingested,
    }

    body = json.dumps(
        state,
        ensure_ascii=False,
        indent=2,
    ).encode("utf-8")

    client.put_object(
        Bucket=BRONZE_BUCKET,
        Key=get_state_key(series_name),
        Body=body,
        ContentType="application/json",
    )