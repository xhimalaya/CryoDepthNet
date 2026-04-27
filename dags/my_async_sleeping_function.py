from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timezone
# from sqlalchemy import create_engine, text
# from airflow.settings import SQL_ALCHEMY_CONN
import logging

log = logging.getLogger(__name__)

async def my_async_sleeping_function(random_base):
    print("This is a function that will run within the DAG execution")

with DAG(
    dag_id="my_async_sleeping_function",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=["glacier", "internal_db"],
) as dag:
    for i in range(5):
        async_sleeping_task = PythonOperator(
            task_id=f"async_sleep_for_{i}",
            python_callable=my_async_sleeping_function,
            op_kwargs={"random_base": i / 10},
        )

    async_sleeping_task