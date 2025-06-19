from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone
import csv

def _get_data(table_schema):
    pg_hook = PostgresHook(
        postgres_conn_id="my_postgres_conn",
        schema="postgres"
    )
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    sql = f"""
        SELECT * FROM information_schema.tables 
        WHERE table_schema = '{table_schema}'
    """
    cursor.execute(sql)
    rows = cursor.fetchall()

    # สร้างไฟล์ CSV
    output_path = f"/opt/airflow/dags/output_{table_schema}.csv"
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([desc[0] for desc in cursor.description])  # เขียน header
        writer.writerows(rows)

    print(f"Exported result to: {output_path}")

with DAG(
    dag_id="week_2_play_with_airflow_connections_and_hooks_v2",
    schedule="@daily",
    start_date=timezone.datetime(2024, 1, 20),
    catchup=False,
):

    get_data = PythonOperator(
        task_id="get_data",
        python_callable=_get_data,
        op_kwargs={"table_schema": "information_schema"},
    )