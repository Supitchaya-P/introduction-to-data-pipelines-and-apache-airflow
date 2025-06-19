from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone

with DAG(
    "week_2_everyday_dag",
    schedule="0 0 * * *",
    start_date=timezone.datetime(2025,6,19),
    catchup=False,
):
    my_first_task = EmptyOperator(task_id="my_first_task")
    my_second_task = EmptyOperator(task_id="my_second_task")

    my_first_task >> my_second_task