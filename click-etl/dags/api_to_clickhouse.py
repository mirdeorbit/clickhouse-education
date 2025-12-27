from airflow import DAG
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests

def load_data():
    response = requests.get("https://jsonplaceholder.typicode.com/posts")
    data = response.json()
    rows = [
        (item['userId'], item['id'], item['title'], item['body'])
        for item in data
    ]
    ch_hook = ClickHouseHook(clickhouse_conn_id='clickhouse_default')
    ch_hook.execute('INSERT INTO etl.posts VALUES', rows)

with DAG(
    dag_id='load_api_data',
    start_date=days_ago(2),
    schedule_interval=None,
    catchup=False
) as dag:

    load_task = PythonOperator(
        task_id='load_api_data_task',
        python_callable=load_data,
    )

# with DAG(
#         dag_id='load_api_data',
#         start_date=days_ago(2)
# ) as dag:
#     dag >> PythonOperator(
#         task_id='load_api_data',
#         python_callable=load_data,
#     )