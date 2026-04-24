from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id="glacier_polygon_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="glacier_db",
        sql="""
        CREATE TABLE IF NOT EXISTS public."GlaciearPloygon"
        (
            name TEXT PRIMARY KEY,
            polygon JSON
        );
        """,
    )

    insert_data = PostgresOperator(
        task_id="insert_data",
        postgres_conn_id="glacier_db",
        sql="""
        INSERT INTO public."GlaciearPloygon" (name, polygon) VALUES
        ('Aletsch Glacier','{"type":"Polygon","coordinates":[[[7.9,46.3],[8.1,46.3],[8.1,46.5],[7.9,46.5],[7.9,46.3]]]}')
        ON CONFLICT (name) DO NOTHING;
        """,
    )

    create_table >> insert_data