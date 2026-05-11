from datetime import datetime
import logging

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from sqlalchemy import text

log = logging.getLogger(__name__)


def insert_glacier_polygons():

    hook = PostgresHook(postgres_conn_id="glacier_db")
    engine = hook.get_sqlalchemy_engine()
    with engine.begin() as conn:
        log.info("Creating table GlaciearPloygon if not exists...")
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS public."GlaciearPloygon"
            (
                name TEXT PRIMARY KEY,
                polygon JSON
            );
        """))
        log.info("Table 'GlaciearPloygon' created or already exists")
        log.info("Inserting glacier polygon data...")
        conn.execute(text("""
            INSERT INTO public."GlaciearPloygon" (name, polygon) VALUES

            ('Aletsch Glacier',
            '{"type":"Polygon","coordinates":[[[7.9,46.3],[8.1,46.3],[8.1,46.5],[7.9,46.5],[7.9,46.3]]]}'),

            ('Oberaletsch Glacier',
            '{"type":"Polygon","coordinates":[[[7.7,46.2],[7.9,46.2],[7.9,46.4],[7.7,46.4],[7.7,46.2]]]}'),

            ('Columbia Glacier',
            '{"type":"Polygon","coordinates":[[[-147.3,61.0],[-146.8,61.0],[-146.8,61.5],[-147.3,61.5],[-147.3,61.0]]]}'),

            ('Juneau Icefield',
            '{"type":"Polygon","coordinates":[[[-135.8,58.2],[-134.8,58.2],[-134.8,59.2],[-135.8,59.2],[-135.8,58.2]]]}'),

            ('Jakobshavn Glacier',
            '{"type":"Polygon","coordinates":[[[-50.5,69.0],[-48.5,69.0],[-48.5,70.5],[-50.5,70.5],[-50.5,69.0]]]}'),

            ('Helheim Glacier',
            '{"type":"Polygon","coordinates":[[[-39.5,65.5],[-37.5,65.5],[-37.5,67.0],[-39.5,67.0],[-39.5,65.5]]]}'),

            ('Vatnajökull Glacier',
            '{"type":"Polygon","coordinates":[[[-17.5,63.8],[-15.5,63.8],[-15.5,65.0],[-17.5,65.0],[-17.5,63.8]]]}'),

            ('Zongo Glacier',
            '{"type":"Polygon","coordinates":[[[-68.3,-16.4],[-68.0,-16.4],[-68.0,-16.1],[-68.3,-16.1],[-68.3,-16.4]]]}'),

            ('Chhota Shigri Glacier',
            '{"type":"Polygon","coordinates":[[[77.3,32.1],[77.6,32.1],[77.6,32.4],[77.3,32.4],[77.3,32.1]]]}'),

            ('Khumbu Glacier',
            '{"type":"Polygon","coordinates":[[[86.7,27.8],[87.0,27.8],[87.0,28.1],[86.7,28.1],[86.7,27.8]]]}')

            ON CONFLICT (name) DO NOTHING;
        """))
        log.info("Glacier polygon data inserted successfully")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
}


with DAG(
    dag_id="Dag_insert_polygon",
    default_args=default_args,
    description="Insert glacier polygon data into CryoDepthNetDb",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["glacier", "polygon", "cryo"],
) as dag:

    insert_polygon_task = PythonOperator(
        task_id="insert_glacier_polygon_data",
        python_callable=insert_glacier_polygons,
    )

    insert_polygon_task