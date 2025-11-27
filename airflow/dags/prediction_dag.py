# airflow/dags/prediction_dag.py
from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path

import pandas as pd
import requests
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine


# -------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------
GOOD_DIR = Path("/opt/airflow/Data/good_data")

FASTAPI_URL = os.environ.get("FASTAPI_URL", "http://fastapi:8000/predict")
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg2://admin:admin@db:5432/defence_db",
)

CHUNK_SIZE = 500


# -------------------------------------------------------------
# HELPER — fetch processed files from DB
# -------------------------------------------------------------
def _get_processed_files():
    engine = create_engine(DATABASE_URL)
    try:
        df = pd.read_sql("SELECT DISTINCT source_file FROM predictions", engine)
        return set(df["source_file"].dropna().tolist())
    except Exception:
        return set()


# -------------------------------------------------------------
# TASK 1 — detect NEW files in good_data
# -------------------------------------------------------------
def check_for_new_data(**context):
    all_files = [f.name for f in GOOD_DIR.glob("*.csv")]
    processed_files = _get_processed_files()

    new_files = [f for f in all_files if f not in processed_files]

    if not new_files:
        raise AirflowSkipException("No new ingested files in good_data → skipping DAG.")

    # send to next task
    context["ti"].xcom_push(key="new_files", value=new_files)
    return new_files


# -------------------------------------------------------------
# TASK 2 — make predictions from FastAPI
# -------------------------------------------------------------
def make_predictions(**context):
    ti = context["ti"]
    new_files = ti.xcom_pull(key="new_files") or []

    if not new_files:
        print("No new files to process.")
        return

    for file_name in new_files:
        file_path = GOOD_DIR / file_name
        print("\n======================================")
        print(f"📌 Processing file: {file_name}")

        if not file_path.exists():
            print(f"❌ File disappeared: {file_path}")
            continue

        try:
            # If file has no rows → skip safely
            try:
                first_rows = pd.read_csv(file_path, nrows=1)
                if first_rows.empty:
                    print(f"⚠️ EMPTY FILE → Skipping {file_name}")
                    continue
            except Exception as e:
                print(f"❌ Could not read file {file_name}: {e}")
                continue

            # process in chunks
            for chunk in pd.read_csv(file_path, chunksize=CHUNK_SIZE):

                if chunk.empty:
                    print(f"⚠️ Empty chunk → skipped (file: {file_name})")
                    continue

                # sanitize for JSON
                chunk = chunk.replace([float("inf"), float("-inf")], None)
                chunk = chunk.where(pd.notnull(chunk), None)

                payload = chunk.to_dict(orient="records")

                print(f"➡ Sending {len(chunk)} rows to FastAPI from {file_name}")

                response = requests.post(
                    FASTAPI_URL,
                    json=payload,
                    params={"source": "scheduled", "source_file": file_name},
                    timeout=60,
                )

                # If FastAPI returns error, show response body
                if response.status_code >= 400:
                    print("\n❌ FastAPI rejected file:", file_name)
                    print("🔍 FastAPI Response Body:")
                    print(response.text)
                    print("======================================")
                    response.raise_for_status()

                print(f"✅ Predicted {len(chunk)} rows for {file_name}")

        except Exception as e:
            print("\n❌ Unexpected error while processing:", file_name)
            print("Error details:", str(e))
            print("======================================")
            raise


# -------------------------------------------------------------
# BUILD DAG
# -------------------------------------------------------------
def build_dag():
    default_args = {"owner": "airflow", "retries": 0}

    with DAG(
        "prediction_dag",
        default_args=default_args,
        description="Scheduled prediction pipeline",
        schedule_interval=timedelta(minutes=2),
        start_date=days_ago(1),
        catchup=False,
        tags=["prediction"],
    ) as dag:

        check_task = PythonOperator(
            task_id="check_for_new_data",
            python_callable=check_for_new_data,
            provide_context=True,
        )

        predict_task = PythonOperator(
            task_id="make_predictions",
            python_callable=make_predictions,
            provide_context=True,
        )

        check_task >> predict_task

    return dag


dag = build_dag()
