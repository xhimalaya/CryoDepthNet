from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timezone
import logging
import time

log = logging.getLogger(__name__)


def my_sleeping_function(random_base: float) -> None:
    """Sleeps for a duration based on random_base, then logs completion."""
    sleep_duration = 1 + random_base  # e.g. 1.0s, 1.1s, 1.2s ...
    log.info("Task starting — will sleep for %.1f seconds", sleep_duration)
    time.sleep(sleep_duration)
    log.info("Task done after %.1f seconds", sleep_duration)


with DAG(
    dag_id="my_async_sleeping_function",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=["glacier", "internal_db"],
) as dag:

    for i in range(5):
        PythonOperator(
            task_id=f"async_sleep_for_{i}",
            python_callable=my_sleeping_function,
            op_kwargs={"random_base": i / 10},
        )