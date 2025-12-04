from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ROOT = "/opt/metar-pipeline"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="metar_pipeline",
    default_args=default_args,
    description="Сбор METAR каждые 30 минут",
    schedule_interval="*/30 * * * *",  # каждые 30 минут
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Сбор METAR - Mongo
    collect_task = BashOperator(
        task_id="collect_metar",
        bash_command=(
            'pip install --quiet pymongo python-dotenv requests psycopg2-binary && '
            f'export PYTHONPATH="{PROJECT_ROOT}"; '
            f'python {PROJECT_ROOT}/src/collector/main.py'
        ),
    )

    # Mongo - Postgres
    etl_task = BashOperator(
        task_id="mongo_to_postgres",
        bash_command=(
            'pip install --quiet pymongo python-dotenv requests psycopg2-binary && '
            f'export PYTHONPATH="{PROJECT_ROOT}"; '
            f'python {PROJECT_ROOT}/src/etl/mongo_to_postgres.py'
        ),
    )

    collect_task >> etl_task
