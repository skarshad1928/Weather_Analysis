from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


# ============================================================
# Default arguments
# ============================================================

default_args = {
    "owner": "weather-data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id="weather_collection_dag",

    default_args=default_args,

    description="Collect weather data every 10 minutes",

    start_date=datetime(2026, 9, 1),

    schedule="*/10 * * * *",

    catchup=False,

    tags=[
        "weather",
        "mongodb",
        "collection"
    ],

) as dag:

    collect_weather = BashOperator(

        task_id="collect_weather",

        bash_command=(
            "python "
            "/opt/airflow/scripts/weather_data_collection.py"
        ),
    )


    collect_weather