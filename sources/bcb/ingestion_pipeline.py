import sys

from datetime import datetime, timezone, timedelta

from .api_client import get_series
from config.bcb_series import BCB_SERIES
from .bronze_storage import save_raw
from .ingestion_state import (
    get_last_reference_date,
    update_state,
)


def format_bcb_date(date):
    return date.strftime("%d/%m/%Y")


def parse_bcb_date(date_string):
    return datetime.strptime(
        date_string,
        "%d/%m/%Y",
    ).date()


def parse_iso_date(date_string):
    return datetime.strptime(
        date_string,
        "%Y-%m-%d",
    ).date()


def get_data(
    series_code,
    frequency,
    start_date,
    end_date,
):
    data = []

    current_start = start_date

    while current_start <= end_date:

        if frequency == "monthly":

            current_end = min(
                current_start + timedelta(days=1825),
                end_date,
            )

        elif frequency == "daily":

            current_end = min(
                current_start + timedelta(days=3652),
                end_date,
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
            end_date=format_bcb_date(current_end),
        )

        data.extend(chunk)

        current_start = current_end + timedelta(days=1)

    return data


def ingest(
    ingestion_date,
    batch_id,
):
    """
    Executa a ingestão incremental das séries do BCB.

    ingestion_date:
        Data em que a ingestão está sendo executada.

    batch_id:
        Identificador único da execução da DAG.
        O mesmo batch_id será utilizado pela etapa
        Bronze -> Silver.
    """

    today = datetime.now(
        timezone.utc
    ).date()

    failed_series = []

    print("")
    print("=" * 60)
    print("Starting BCB ingestion")
    print(f"Ingestion date: {ingestion_date}")
    print(f"Batch ID: {batch_id}")
    print("=" * 60)

    for series_name, series_config in BCB_SERIES.items():

        print("")
        print("-" * 60)
        print(f"Starting ingestion: {series_name}")
        print(f"Ingestion date: {ingestion_date}")
        print(f"Batch ID: {batch_id}")
        print("-" * 60)

        try:

            last_reference_date = get_last_reference_date(
                series_name
            )

            if last_reference_date:

                last_date = parse_bcb_date(
                    last_reference_date
                )

                # Começamos na última data conhecida.
                #
                # A filtragem abaixo garante que a própria
                # última data não seja novamente gravada.
                start_date = last_date

                print(
                    f"Last reference date: {last_date}"
                )

            else:

                last_date = None

                start_date = parse_iso_date(
                    series_config["available_from"]
                )

                print(
                    "No previous state found."
                )

                print(
                    f"Starting from: {start_date}"
                )

            if start_date > today:

                print(
                    "Start date is after today."
                )

                print(
                    "Skipping series."
                )

                continue


            data = get_data(
                series_code=series_config["code"],
                frequency=series_config["frequency"],
                start_date=start_date,
                end_date=today,
            )

            if not data:

                print(
                    "API returned no data."
                )

                print(
                    "Skipping series."
                )

                continue


            new_data = []

            for row in data:

                reference_date = parse_bcb_date(
                    row["data"]
                )

                if last_date is None:

                    new_data.append(row)

                elif reference_date > last_date:

                    new_data.append(row)


            if not new_data:

                print(
                    "No new data found."
                )

                print(
                    "Skipping series."
                )

                continue

            print(
                f"New rows: {len(new_data)}"
            )


            save_raw(
                data=new_data,
                series_name=series_name,
                ingestion_date=ingestion_date,
                batch_id=batch_id,
            )

            print(
                "Bronze saved successfully."
            )


            latest_date = max(
                parse_bcb_date(
                    row["data"]
                )
                for row in new_data
            )


            update_state(
                series_name=series_name,
                last_reference_date=latest_date.strftime("%d/%m/%Y"),
                ingestion_date=ingestion_date,
                rows_ingested=len(new_data),
            )


            print(
                f"State updated to: {latest_date}"
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
            "Ingestion failed for: "
            + ", ".join(failed_series)
        )

    print("")
    print("=" * 60)
    print("Ingestion completed successfully.")
    print(f"Batch ID: {batch_id}")
    print("=" * 60)


if __name__ == "__main__":

    if len(sys.argv) != 3:

        raise ValueError(
            "Usage: "
            "ingestion_pipeline.py "
            "<ingestion_date> "
            "<batch_id>"
        )

    ingestion_date = sys.argv[1]
    batch_id = sys.argv[2]

    ingest(
        ingestion_date=ingestion_date,
        batch_id=batch_id,
    )