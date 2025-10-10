"""
Simple Ingestion DAG (Follow-up Session 1)
Moves one random file from Data/raw → Data/good_data
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os, random, shutil
from airflow.utils.dates import days_ago

# Adjust these if needed via env vars in docker-compose
RAW_DIR = os.environ.get("RAW_DATA_DIR", "/opt/airflow/Data/raw")
GOOD_DIR = os.environ.get("GOOD_DATA_DIR", "/opt/airflow/Data/good_data")

default_args = {
    "owner": "team",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="data_ingestion_dag",
    default_args=default_args,
    schedule_interval="*/1 * * * *",  # runs every minute for demo
    start_date=days_ago(1),
    catchup=False,
    tags=["ingestion", "followup1"],
) as dag:

    def read_data(**context):
        if not os.path.exists(RAW_DIR):
            raise FileNotFoundError(f"raw-data folder not found: {RAW_DIR}")

        files = [f for f in os.listdir(RAW_DIR) if f.endswith(".csv")]
        if not files:
            context['ti'].xcom_push(key="picked_file", value=None)
            return None

        picked_file = random.choice(files)
        full_path = os.path.join(RAW_DIR, picked_file)
        context['ti'].xcom_push(key="picked_file", value=full_path)
        return full_path

    def save_file(**context):
        picked = context['ti'].xcom_pull(key="picked_file")
        if not picked:
            return "no_file"

        os.makedirs(GOOD_DIR, exist_ok=True)
        dest = os.path.join(GOOD_DIR, os.path.basename(picked))
        shutil.move(picked, dest)
        return dest

    read_task = PythonOperator(
        task_id="read_data",
        python_callable=read_data,
        provide_context=True
    )

    save_task = PythonOperator(
        task_id="save_file",
        python_callable=save_file,
        provide_context=True
    )

    read_task >> save_task
