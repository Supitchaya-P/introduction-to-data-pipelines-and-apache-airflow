import json

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone

import requests

DAG_FOLDER = "/opt/airflow/dags"

def _get_weather_data():
    # API_KEY = os.environ.get("WEATHER_API_KEY")
    API_KEY = Variable.get("weather_api_key")

    payload = {
        "q": "bangkok",
        "appid": API_KEY,
        "units": "metric"
    }
    url = "https://api.openweathermap.org/data/2.5/weather"
    response = requests.get(url, params=payload)
    print(response.url)

    data = response.json()
    print(data)

    with open(f"{DAG_FOLDER}/data.json", "w") as f:
        json.dump(data, f)

def _validate_data():
    with open(f"{DAG_FOLDER}/data.json", "r") as f:
        data = json.load(f)

    assert data.get("main") is not None

def _create_weather_table():
    pg_hook = PostgresHook(
        postgres_conn_id="weather_postgres_conn",
        schema="postgres"
    )
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    sql = """
        CREATE TABLE IF NOT EXISTS weathers (
            dt BIGINT NOT NULL,
            temp FLOAT NOT NULL
        )
    """
    cursor.execute(sql)
    connection.commit()

def _load_data_to_postgres():
    pg_hook = PostgresHook(
        postgres_conn_id="weather_postgres_conn",
        schema="postgres"
    )
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    with open(f"{DAG_FOLDER}/data.json", "r") as f:
        data = json.load(f)

    temp = data["main"]["temp"]
    dt = data["dt"]
    sql = f"""
        INSERT INTO weathers (dt, temp) VALUES ({dt}, {temp})
    """
    cursor.execute(sql)
    connection.commit()

with DAG(
    "week_3_weather_api_dag",
    schedule="0 */3 * * *",
    start_date=timezone.datetime(2025, 6, 19),
    tags=["dpu"],
):
    start = EmptyOperator(task_id="start")

    get_weather_data = PythonOperator(
        task_id="get_weather_data",
        python_callable=_get_weather_data,
    )

    validate_data = PythonOperator(
        task_id="validate_data",
        python_callable=_validate_data,
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

    start >> get_weather_data >> validate_data >> load_data_to_postgres >> end
    start >> create_weather_table >> load_data_to_postgres