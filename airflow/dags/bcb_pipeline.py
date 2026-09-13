from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="bcb_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@monthly",
    catchup=True,
    default_args=default_args,
    tags=["bcb"],
) as dag:

    ingestion_date = "{{ logical_date.strftime('%Y-%m-%d') }}"

    batch_id = (
        "{{ run_id "
        "| replace(':', '_') "
        "| replace('+', '_') "
        "| replace('/', '_') }}"
    )

    ingestion = BashOperator(
        task_id="ingestion",
        bash_command=(
            "docker exec ingestion "
            "python -m sources.bcb.ingestion_pipeline "
            f"{ingestion_date} "
            f"{batch_id}"
        ),
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            "docker exec spark "
            "/opt/spark/bin/spark-submit "
            "/opt/spark-apps/jobs/bcb/bronze_to_silver.py "
            f"{ingestion_date} "
            f"{batch_id}"
        ),
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=(
            "docker exec spark "
            "/opt/spark/bin/spark-submit "
            "/opt/spark-apps/jobs/bcb/silver_to_gold.py"
        ),
    )

    gold_to_postgres = BashOperator(
        task_id="gold_to_postgres",
        bash_command=(
            "docker exec spark "
            "/opt/spark/bin/spark-submit "
            "/opt/spark-apps/jobs/bcb/gold_to_postgres.py"
        ),
    )

    dbt = BashOperator(
        task_id="dbt",
        bash_command="docker exec dbt dbt build",
    )

    ingestion >> bronze_to_silver
    bronze_to_silver >> silver_to_gold
    silver_to_gold >> gold_to_postgres
    gold_to_postgres >> dbt