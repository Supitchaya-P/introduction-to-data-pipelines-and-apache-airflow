import json

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone

import great_expectations as gx
import pandas as pd
import requests
from great_expectations.dataset import PandasDataset

def _get_weather_data(**context):
    API_KEY = Variable.get("weather_api_key")
    payload = {
        "q": "bangkok",
        "appid": API_KEY,
        "units": "metric"
    }
    url = "https://api.openweathermap.org/data/2.5/weather"
    response = requests.get(url, params=payload)
    data = response.json()
    timestamp = context["execution_date"]
    output_file = f"/opt/airflow/dags/weather_data_{timestamp}.json"
    with open(output_file, "w") as f:
        json.dump(data, f)
    return output_file

def _validate_temperature(**context):
    ti = context["ti"]
    file_name = ti.xcom_pull(task_ids="get_weather_data", key="return_value")

    with open(file_name, "r") as f:
        data = json.load(f)

    df = pd.DataFrame([data["main"]])
    dataset = PandasDataset(df)

    result = dataset.expect_column_values_to_be_between(
        column="temp",
        min_value=20,
        max_value=40,
    )
    # print(result)
    assert result["success"] is True

def _create_weather_table(**context):
    pg_hook = PostgresHook(
        postgres_conn_id="my_postgres_conn",
        schema="postgres"
    )
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS weathers (
            dt BIGINT NOT NULL,
            temp FLOAT NOT NULL
        )
    """)
    connection.commit()

def _load_data_to_postgres(**context):
    ti = context["ti"]
    file_name = ti.xcom_pull(task_ids="get_weather_data", key="return_value")

    with open(file_name, "r") as f:
        data = json.load(f)

    temp = data["main"]["temp"]
    dt = data["dt"]
    pg_hook = PostgresHook(
        postgres_conn_id="my_postgres_conn",
        schema="postgres"
    )
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(f"INSERT INTO weathers (dt, temp) VALUES ({dt}, {temp})")
    connection.commit()

default_args = {
    "email": ["supitchaya.p99@gmail.com"],
}

with DAG(
    "week_3_weather_api_dag_v2",
    default_args=default_args,
    schedule="@hourly",
    start_date=timezone.datetime(2025, 6, 19),
    catchup=False,
):
    start = EmptyOperator(task_id="start")

    get_weather_data = PythonOperator(
        task_id="get_weather_data",
        python_callable=_get_weather_data,
    )

    validate_temperature = PythonOperator(
        task_id="validate_temperature",
        python_callable=_validate_temperature,
    )

    create_weather_table = PythonOperator(
        task_id="create_weather_table",
        python_callable=_create_weather_table,
    )

    load_data_to_postgres = PythonOperator(
        task_id="load_data_to_postgres",
        python_callable=_load_data_to_postgres,
    )

    end = EmptyOperator(task_id="end")

    start >> get_weather_data >> validate_temperature >> create_weather_table >> load_data_to_postgres >> end