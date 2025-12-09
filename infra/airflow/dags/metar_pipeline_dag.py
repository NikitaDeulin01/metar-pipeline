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
    schedule_interval="*/30 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Сбор METAR Mongo
    collect_task = BashOperator(
        task_id="collect_metar",
        bash_command=(
            'pip install --quiet pymongo python-dotenv requests psycopg2-binary && '
            f'export PYTHONPATH="{PROJECT_ROOT}"; '
            f'python {PROJECT_ROOT}/src/collector/main.py'
        ),
    )

    # Mongo to Postgres (STG)
    etl_task = BashOperator(
        task_id="mongo_to_postgres",
        bash_command=(
            'pip install --quiet pymongo python-dotenv requests psycopg2-binary && '
            f'export PYTHONPATH="{PROJECT_ROOT}"; '
            f'python {PROJECT_ROOT}/src/etl/mongo_to_postgres.py'
        ),
    )

    # dbt build
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            'curl -s -X POST http://host.docker.internal:8000/dbt/build || exit 1'
        ),
    )

    elementary_report = BashOperator(
        task_id="elementary_report",
        bash_command=(
            'curl -s -X POST http://host.docker.internal:8000/dbt/report || exit 1'
        ),
    )

    collect_task >> etl_task >> dbt_build >> elementary_report
