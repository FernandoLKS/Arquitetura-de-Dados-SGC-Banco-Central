from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="bcb_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@monthly",
    catchup=False,
    tags=["bcb"],
) as dag:

    ingestion = BashOperator(
        task_id="bcb_ingestion",
        bash_command=(
            "python -m sources.bcb.ingestion_pipeline"
        ),
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            "docker exec spark "
            "/opt/spark/bin/spark-submit "
            "--conf spark.log.level=WARN "
            "/opt/spark-apps/jobs/bcb/bronze_to_silver.py "
            "{{ ds }}"
        ),
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=(
            "docker exec spark "
            "/opt/spark/bin/spark-submit "
            "--conf spark.log.level=WARN "
            "/opt/spark-apps/jobs/bcb/silver_to_gold.py"
        ),
    )

    gold_to_postgres = BashOperator(
        task_id="gold_to_postgres",
        bash_command=(
            "docker exec spark "
            "/opt/spark/bin/spark-submit "
            "--conf spark.log.level=WARN "
            "/opt/spark-apps/jobs/bcb/gold_to_postgres.py"
        ),
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="docker exec dbt dbt build",
    )

    ingestion >> bronze_to_silver
    bronze_to_silver >> silver_to_gold
    silver_to_gold >> gold_to_postgres
    gold_to_postgres >> dbt_build