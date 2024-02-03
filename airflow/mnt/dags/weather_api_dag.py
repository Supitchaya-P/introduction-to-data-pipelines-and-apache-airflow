from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone

def _get_weather_data():
    import requests

    API_KEY = "59d496bd13be31bf70420fe42659a796"
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

with DAG(
    "weather_api_dag",
    schedule="@hourly",
    start_date=timezone.datetime(2024,1,27),
):
    #Code here
    start = EmptyOperator(task_id="start")

    get_weather_data = PythonOperator(
        task_id="get_weather_data",
        python_callable=_get_weather_data,
    )

    end = EmptyOperator(task_id="end")

    start >> end
