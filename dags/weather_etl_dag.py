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
    "retry_delay": timedelta(minutes=5),
}


# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id="weather_etl_dag",

    default_args=default_args,

    description=(
        "Daily Weather MongoDB to "
        "Supabase PostgreSQL ETL using PySpark"
    ),

    start_date=datetime(2026, 9, 1),

    schedule="0 0 * * *",

    catchup=False,

    tags=[
        "weather",
        "pyspark",
        "mongodb",
        "postgresql",
        "supabase"
    ],

) as dag:

    run_weather_etl = BashOperator(

        task_id="run_weather_etl",

        bash_command=(
            "python "
            "/opt/airflow/scripts/weather_etl.py"
        ),
    )


    run_weather_etl