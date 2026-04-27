from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timezone
from sqlalchemy import create_engine, text
from airflow.settings import SQL_ALCHEMY_CONN
import logging

log = logging.getLogger(__name__)

def run_sql():
    log.info("=" * 80)
    log.info("Starting run_sql task")
    log.info("=" * 80)

    try:
        log.info("Reading SQL_ALCHEMY_CONN from Airflow settings")
        log.debug("SQL_ALCHEMY_CONN: %s", SQL_ALCHEMY_CONN)

        log.info("Creating SQLAlchemy engine...")
        engine = create_engine(SQL_ALCHEMY_CONN)
        log.info("Engine created successfully: %s", engine)

    except Exception as e:
        log.error("Failed to create database engine: %s", str(e), exc_info=True)
        raise

    try:
        log.info("Connecting to database and creating table if not exists...")
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS public."GlaciearPloygon"
                (
                    name TEXT PRIMARY KEY,
                    polygon JSON
                );
            """))
            log.info("Table 'GlaciearPloygon' created or already exists")

            log.info("Inserting glacier polygon data...")
            result = conn.execute(text("""
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
            log.info("Insert complete. Rows affected: %s", result.rowcount)

    except Exception as e:
        log.error("Database operation failed: %s", str(e), exc_info=True)
        raise

    finally:
        log.info("Disposing engine and closing connections...")
        engine.dispose()
        log.info("Engine disposed successfully")

    log.info("=" * 80)
    log.info("run_sql task completed successfully")
    log.info("=" * 80)


with DAG(
    dag_id="airflow_internal_db_glacier",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=["glacier", "internal_db"],
) as dag:
    task = PythonOperator(
        task_id="run_sql_internal_db",
        python_callable=run_sql,
    )