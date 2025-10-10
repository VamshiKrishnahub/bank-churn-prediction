"""
Simple Ingestion DAG (Follow-up Session 1)
Moves one random file from Data/raw → Data/good_data / Data/bad_data (after validation)
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os, random, shutil
from airflow.utils.dates import days_ago
import pandas as pd  # NEW: Required for validation

# Adjust these if needed via env vars in docker-compose
RAW_DIR = os.environ.get("RAW_DATA_DIR", "/opt/airflow/Data/raw")
GOOD_DIR = os.environ.get("GOOD_DATA_DIR", "/opt/airflow/Data/good_data")
BAD_DIR = os.environ.get("BAD_DATA_DIR", "/opt/airflow/Data/bad_data")  # NEW

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

    def validate_and_save(**context):
        picked = context['ti'].xcom_pull(key="picked_file")
        if not picked:
            return "no_file"

        os.makedirs(GOOD_DIR, exist_ok=True)
        os.makedirs(BAD_DIR, exist_ok=True)

        df = pd.read_csv(picked)

        # ---- VALIDATION RULE: Drop rows with missing values ----
        good_df = df.dropna()
        bad_df = df[df.isnull().any(axis=1)]

        file_name = os.path.basename(picked)

        if not good_df.empty:
            good_path = os.path.join(GOOD_DIR, file_name)
            good_df.to_csv(good_path, index=False)
            print(f"Saved GOOD data → {good_path}")

        if not bad_df.empty:
            bad_path = os.path.join(BAD_DIR, f"BAD_{file_name}")
            bad_df.to_csv(bad_path, index=False)
            print(f"Saved BAD data → {bad_path}")

        os.remove(picked)  # Remove original raw file
        return {"good_rows": len(good_df), "bad_rows": len(bad_df)}

    read_task = PythonOperator(
        task_id="read_data",
        python_callable=read_data,
        provide_context=True
    )

    validate_task = PythonOperator(
        task_id="validate_and_save",
        python_callable=validate_and_save,
        provide_context=True
    )

    read_task >> validate_task
