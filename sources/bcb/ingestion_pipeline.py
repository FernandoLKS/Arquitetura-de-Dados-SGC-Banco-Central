import sys

from datetime import datetime, timezone, timedelta

from .api_client import get_series
from config.bcb_series import BCB_SERIES
from .bronze_storage import (
    save_raw_staging,
    copy_previous_batch,
    commit_batch,
    delete_staging_batch,
)
from .ingestion_state import (
    get_state,
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
                current_start + timedelta(days=730),
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

        current_start = (
            current_end + timedelta(days=1)
        )

    return data


def ingest(
    ingestion_date,
    batch_id,
):

    today = datetime.now(
        timezone.utc
    ).date()

    pending_data = []

    pending_states = []

    batch_series_status = {}

    print("")
    print("=" * 60)
    print("Starting BCB ingestion")
    print(f"Ingestion date: {ingestion_date}")
    print(f"Batch ID: {batch_id}")
    print("=" * 60)

    try:

        for series_name, series_config in BCB_SERIES.items():

            print("")
            print("-" * 60)
            print(f"Starting ingestion: {series_name}")
            print("-" * 60)

            try:

                state = get_state(
                    series_name
                )

                if state:

                    last_reference_date = (
                        state.get(
                            "last_reference_date"
                        )
                    )

                    last_batch_id = (
                        state.get(
                            "last_batch_id"
                        )
                    )

                    last_ingestion_date = (
                        state.get(
                            "last_ingestion_date"
                        )
                    )

                    last_date = parse_bcb_date(
                        last_reference_date
                    )

                    start_date = last_date

                    print(
                        f"Last reference date: "
                        f"{last_date}"
                    )

                    print(
                        f"Previous batch: "
                        f"{last_batch_id}"
                    )

                else:

                    last_reference_date = None
                    last_batch_id = None
                    last_ingestion_date = None
                    last_date = None

                    start_date = parse_iso_date(
                        series_config[
                            "available_from"
                        ]
                    )

                    print(
                        "No previous state found."
                    )

                    print(
                        f"Starting from: "
                        f"{start_date}"
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

                new_data = []

                for row in data:

                    reference_date = parse_bcb_date(
                        row["data"]
                    )

                    if last_date is None:

                        new_data.append(row)

                    elif reference_date > last_date:

                        new_data.append(row)

                if new_data:

                    print(
                        f"New rows: "
                        f"{len(new_data)}"
                    )

                    latest_date = max(
                        parse_bcb_date(
                            row["data"]
                        )
                        for row in new_data
                    )

                    pending_data.append(
                        {
                            "series_name": series_name,
                            "data": new_data,
                        }
                    )

                    pending_states.append(
                        {
                            "series_name": series_name,
                            "last_reference_date": (
                                latest_date.strftime(
                                    "%d/%m/%Y"
                                )
                            ),
                            "rows_ingested": len(
                                new_data
                            ),
                        }
                    )

                    batch_series_status[
                        series_name
                    ] = "new_data"

                else:

                    print(
                        "No new data found."
                    )

                    if (
                        last_batch_id
                        and last_ingestion_date
                    ):

                        print(
                            "Reusing previous batch."
                        )

                        pending_data.append(
                            {
                                "series_name": series_name,
                                "previous_batch": True,
                                "previous_batch_id": (
                                    last_batch_id
                                ),
                                "previous_ingestion_date": (
                                    last_ingestion_date
                                ),
                            }
                        )

                        pending_states.append(
                            {
                                "series_name": series_name,
                                "last_reference_date": (
                                    last_reference_date
                                ),
                                "rows_ingested": 0,
                            }
                        )

                        batch_series_status[
                            series_name
                        ] = "no_data"

                    else:

                        raise RuntimeError(
                            f"No new data and no "
                            f"previous batch found "
                            f"for {series_name}"
                        )

            except Exception as error:

                print(
                    f"Error processing "
                    f"{series_name}: {error}"
                )

                raise RuntimeError(
                    f"Ingestion failed for: "
                    f"{series_name}"
                ) from error

        print("")
        print("=" * 60)
        print("Building staging batch...")
        print("=" * 60)

        for item in pending_data:

            if item.get(
                "previous_batch",
                False,
            ):

                copy_previous_batch(
                    series_name=item[
                        "series_name"
                    ],
                    previous_ingestion_date=item[
                        "previous_ingestion_date"
                    ],
                    previous_batch_id=item[
                        "previous_batch_id"
                    ],
                    ingestion_date=ingestion_date,
                    batch_id=batch_id,
                )

            else:

                save_raw_staging(
                    data=item["data"],
                    series_name=item["series_name"],
                    ingestion_date=ingestion_date,
                    batch_id=batch_id,
                )

        print("")
        print("=" * 60)
        print("Publishing Bronze batch...")
        print("=" * 60)

        commit_batch(
            ingestion_date=ingestion_date,
            batch_id=batch_id,
            series_status=batch_series_status,
        )

        print("")
        print("=" * 60)
        print("Updating ingestion states...")
        print("=" * 60)

        for state in pending_states:

            update_state(
                series_name=state[
                    "series_name"
                ],
                last_reference_date=state[
                    "last_reference_date"
                ],
                ingestion_date=ingestion_date,
                batch_id=batch_id,
                rows_ingested=state[
                    "rows_ingested"
                ],
            )

            print(
                f"State updated: "
                f"{state['series_name']}"
            )

        print("")
        print("=" * 60)
        print("Ingestion completed successfully.")
        print(f"Batch ID: {batch_id}")
        print("=" * 60)

    except Exception:

        print("")
        print("=" * 60)
        print("Ingestion failed.")
        print(f"Batch ID: {batch_id}")
        print("Cleaning staging data...")
        print("=" * 60)

        try:

            delete_staging_batch(
                ingestion_date=ingestion_date,
                batch_id=batch_id,
            )

        except Exception as cleanup_error:

            print(
                f"Error cleaning staging: "
                f"{cleanup_error}"
            )

        raise


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