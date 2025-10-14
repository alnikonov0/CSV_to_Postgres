from datetime import datetime, timedelta
from pydoc import describe

from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

from main import ETL

sys.path.append(os.path.dirname(__file__))

def run_etl():
    etl = ETL()
    etl.run(
        file_path="/opt/airflow/dags/test_etl_data_1m.csv",
        batch_size=100000,
        dst_table="stg.clients"
    )

# dag parameters #
default_args = {
    "owner": "airflow",
    "catchup": False,
    "depends_on_past": False,
    "start_date": datetime(2020, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

# DAG #

with DAG(
    dag_id="ETL_DAG",
    default_args=default_args,
    description = 'Тренировочный даг для шедулинга ETL CSV - PG',
    schedule_interval = '@daily',
    catchup = False,
    start_date=datetime(2020, 1, 1),
    tags=['ETL_DAG']
) as dag:

    run_etl = PythonOperator(
        task_id = 'run_etl',
        python_callable=run_etl
    )
