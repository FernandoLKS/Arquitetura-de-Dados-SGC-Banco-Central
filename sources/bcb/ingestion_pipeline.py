from datetime import datetime, timezone, timedelta

from .api_client import get_series
from config.bcb_series import BCB_SERIES
from .bronze_storage import save_raw
from .ingestion_state import (
    get_last_reference_date,
    update_state
)


def format_bcb_date(date):
    return date.strftime("%d/%m/%Y")


def parse_bcb_date(date_string):
    return datetime.strptime(
        date_string,
        "%d/%m/%Y"
    ).date()


def parse_iso_date(date_string):
    return datetime.strptime(
        date_string,
        "%Y-%m-%d"
    ).date()


def get_data(
    series_code,
    frequency,
    start_date,
    end_date
):

    data = []
    current_start = start_date

    while current_start <= end_date:

        if frequency == "monthly":

            current_end = min(
                current_start + timedelta(days=1825),
                end_date
            )

        elif frequency == "daily":

            current_end = min(
                current_start + timedelta(days=3652),
                end_date
            )

        else:

            raise ValueError(
                f"Frequency not supported: {frequency}"
            )

        print(
            f"Request window: "
            f"{format_bcb_date(current_start)} -> "
            f"{format_bcb_date(current_end)}"
        )

        chunk = get_series(
            series_code=series_code,
            start_date=format_bcb_date(current_start),
            end_date=format_bcb_date(current_end)
        )

        data.extend(chunk)

        current_start = (
            current_end + timedelta(days=1)
        )

    return data


def ingest():

    ingestion_date = datetime.now(
        timezone.utc
    ).strftime("%Y-%m-%d")

    today = datetime.now(
        timezone.utc
    ).date()

    failed_series = []

    for series_name, series_config in BCB_SERIES.items():

        print("")
        print("=" * 60)
        print(f"Start: {series_name}")
        print(f"Code: {series_config['code']}")
        print(f"Frequency: {series_config['frequency']}")
        print("=" * 60)

        try:

            last_reference_date = get_last_reference_date(
                series_name
            )

            if last_reference_date:

                start_date = (
                    parse_bcb_date(last_reference_date)
                    + timedelta(days=1)
                )

                print(
                    f"Last reference: {last_reference_date}"
                )

            else:

                start_date = parse_iso_date(
                    series_config["available_from"]
                )

                print(
                    "No history found. "
                    "Running initial load."
                )

            if start_date > today:

                print(
                    "No new data available."
                )

                continue

            data = get_data(
                series_code=series_config["code"],
                frequency=series_config["frequency"],
                start_date=start_date,
                end_date=today
            )

            if not data:

                print(
                    "No new data available."
                )

                continue

            if last_reference_date:

                last_reference = parse_bcb_date(
                    last_reference_date
                )

                data = [
                    item
                    for item in data
                    if parse_bcb_date(item["data"])
                    > last_reference
                ]

            if not data:

                print(
                    "No new data available."
                )

                continue

            save_raw(
                data=data,
                series_name=series_name,
                ingestion_date=ingestion_date
            )

            last_date = max(
                parse_bcb_date(item["data"])
                for item in data
            )

            last_date = format_bcb_date(
                last_date
            )

            update_state(
                series_name=series_name,
                last_reference_date=last_date,
                ingestion_date=ingestion_date,
                rows_ingested=len(data)
            )

            print(
                f"Rows ingested: {len(data)}"
            )

            print(
                f"Last reference saved: {last_date}"
            )

            print(
                "Series completed."
            )

        except Exception as error:

            print(
                f"Error ingesting {series_name}: {error}"
            )

            failed_series.append(
                series_name
            )

    if failed_series:

        raise RuntimeError(
            "Ingestion failed for: "
            + ", ".join(failed_series)
        )

    return ingestion_date


if __name__ == "__main__":

    ingestion_date = ingest()

    print(ingestion_date)