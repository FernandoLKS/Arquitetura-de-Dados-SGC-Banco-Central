import sys

from datetime import datetime, timezone, timedelta

from .api_client import get_series
from config.bcb_series import BCB_SERIES
from .bronze_storage import (
    save_raw_staging,
    copy_previous_batch,
    ## commit_batch,
    ## delete_staging_batch,
    write_batch_manifest,
    commit_series,
    delete_staging_series
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

    batch_series_status = {}

    failures = []

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
        print("-" * 60)

        try:

            # --------------------------------------------------
            # 1. Read current state
            # --------------------------------------------------

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

                # Keep your current strategy:
                # re-request the last reference date
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

            # --------------------------------------------------
            # 2. Check if series is already up to date
            # --------------------------------------------------

            if start_date > today:

                print(
                    "Start date is after today."
                )

                print(
                    "Skipping series."
                )

                batch_series_status[
                    series_name
                ] = "up_to_date"

                continue

            # --------------------------------------------------
            # 3. Get data
            # --------------------------------------------------

            data = get_data(
                series_code=series_config["code"],
                frequency=series_config["frequency"],
                start_date=start_date,
                end_date=today,
            )

            # --------------------------------------------------
            # 4. Filter new observations
            # --------------------------------------------------

            new_data = []

            for row in data:

                reference_date = parse_bcb_date(
                    row["data"]
                )

                if last_date is None:

                    new_data.append(row)

                elif reference_date > last_date:

                    new_data.append(row)

            # --------------------------------------------------
            # 5A. New data
            # --------------------------------------------------

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

                # Save this series to staging
                save_raw_staging(
                    data=new_data,
                    series_name=series_name,
                    ingestion_date=ingestion_date,
                    batch_id=batch_id,
                )

                # Publish ONLY this series
                commit_series(
                    series_name=series_name,
                    ingestion_date=ingestion_date,
                    batch_id=batch_id,
                )

                # Update ONLY this series state
                update_state(
                    series_name=series_name,
                    last_reference_date=(
                        latest_date.strftime(
                            "%d/%m/%Y"
                        )
                    ),
                    ingestion_date=ingestion_date,
                    batch_id=batch_id,
                    rows_ingested=len(new_data),
                )

                delete_staging_series(
                    series_name=series_name,
                    ingestion_date=ingestion_date,
                    batch_id=batch_id,
                )

                batch_series_status[
                    series_name
                ] = "success"

                print(
                    f"Series completed: "
                    f"{series_name}"
                )

            # --------------------------------------------------
            # 5B. No new data
            # --------------------------------------------------

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

                    copy_previous_batch(
                        series_name=series_name,
                        previous_ingestion_date=(
                            last_ingestion_date
                        ),
                        previous_batch_id=(
                            last_batch_id
                        ),
                        ingestion_date=ingestion_date,
                        batch_id=batch_id,
                    )

                    # Publish the reused series
                    commit_series(
                        series_name=series_name,
                        ingestion_date=ingestion_date,
                        batch_id=batch_id,
                    )

                    # Keep the existing reference date
                    update_state(
                        series_name=series_name,
                        last_reference_date=(
                            last_reference_date
                        ),
                        ingestion_date=ingestion_date,
                        batch_id=batch_id,
                        rows_ingested=0,
                    )

                    batch_series_status[
                        series_name
                    ] = "no_data"

                    print(
                        f"Series completed: "
                        f"{series_name}"
                    )

                else:

                    raise RuntimeError(
                        f"No new data and no "
                        f"previous batch found "
                        f"for {series_name}"
                    )

        except Exception as error:

            print("")
            print(
                f"ERROR processing "
                f"{series_name}: {error}"
            )

            batch_series_status[
                series_name
            ] = "failed"

            failures.append(
                series_name
            )

            # IMPORTANT:
            # Do NOT update this series state.
            # Do NOT stop the other series.
            continue

    # ----------------------------------------------------------
    # 6. Write batch manifest
    # ----------------------------------------------------------

    if failures:

        write_batch_manifest(
            ingestion_date=ingestion_date,
            batch_id=batch_id,
            series_status=batch_series_status,
            status="failed",
        )

        print("")
        print("=" * 60)
        print("Ingestion completed with failures.")
        print(f"Failed series: {failures}")
        print(f"Batch ID: {batch_id}")
        print("=" * 60)

        raise RuntimeError(
            "BCB ingestion failed for: "
            + ", ".join(failures)
        )

    # ----------------------------------------------------------
    # 7. Entire batch succeeded
    # ----------------------------------------------------------

    write_batch_manifest(
        ingestion_date=ingestion_date,
        batch_id=batch_id,
        series_status=batch_series_status,
        status="committed",
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