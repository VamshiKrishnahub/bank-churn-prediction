# # airflow/dags/prediction_dag.py
# from __future__ import annotations
#
# import os
# from datetime import timedelta
# from pathlib import Path
#
# import pandas as pd
# import requests
# from airflow import DAG
# from airflow.exceptions import AirflowSkipException
# from airflow.operators.python import PythonOperator
# from airflow.utils.dates import days_ago
# from sqlalchemy import create_engine
#
#
# # -------------------------------------------------------------
# # CONFIG
# # -------------------------------------------------------------
# GOOD_DIR = Path("/opt/airflow/Data/good_data")
#
# FASTAPI_URL = os.environ.get("FASTAPI_URL", "http://fastapi:8000/predict")
# DATABASE_URL = os.environ.get(
#     "DATABASE_URL",
#     "postgresql+psycopg2://admin:admin@db:5432/defence_db",
# )
#
# CHUNK_SIZE = 500
#
#
# # -------------------------------------------------------------
# # HELPER — fetch processed files from DB
# # -------------------------------------------------------------
# def _get_processed_files():
#     engine = create_engine(DATABASE_URL)
#     try:
#         df = pd.read_sql("SELECT DISTINCT source_file FROM predictions", engine)
#         return set(df["source_file"].dropna().tolist())
#     except Exception:
#         return set()
#
#
# # -------------------------------------------------------------
# # TASK 1 — detect NEW files in good_data
# # -------------------------------------------------------------
# def check_for_new_data(**context):
#     all_files = [f.name for f in GOOD_DIR.glob("*.csv")]
#     processed_files = _get_processed_files()
#
#     new_files = [f for f in all_files if f not in processed_files]
#
#     if not new_files:
#         raise AirflowSkipException("No new ingested files in good_data → skipping DAG.")
#
#     # send to next task
#     context["ti"].xcom_push(key="new_files", value=new_files)
#     return new_files
#
#
# # -------------------------------------------------------------
# # TASK 2 — make predictions from FastAPI
# # -------------------------------------------------------------
# def make_predictions(**context):
#     ti = context["ti"]
#     new_files = ti.xcom_pull(key="new_files") or []
#
#     if not new_files:
#         print("No new files to process.")
#         return
#
#     for file_name in new_files:
#         file_path = GOOD_DIR / file_name
#         print("\n======================================")
#         print(f"📌 Processing file: {file_name}")
#
#         if not file_path.exists():
#             print(f"❌ File disappeared: {file_path}")
#             continue
#
#         try:
#             # If file has no rows → skip safely
#             try:
#                 first_rows = pd.read_csv(file_path, nrows=1)
#                 if first_rows.empty:
#                     print(f"⚠️ EMPTY FILE → Skipping {file_name}")
#                     continue
#             except Exception as e:
#                 print(f"❌ Could not read file {file_name}: {e}")
#                 continue
#
#             # process in chunks
#             for chunk in pd.read_csv(file_path, chunksize=CHUNK_SIZE):
#
#                 if chunk.empty:
#                     print(f"⚠️ Empty chunk → skipped (file: {file_name})")
#                     continue
#
#                 # sanitize for JSON
#                 chunk = chunk.replace([float("inf"), float("-inf")], None)
#                 chunk = chunk.where(pd.notnull(chunk), None)
#
#                 payload = chunk.to_dict(orient="records")
#
#                 print(f"➡ Sending {len(chunk)} rows to FastAPI from {file_name}")
#
#                 response = requests.post(
#                     FASTAPI_URL,
#                     json=payload,
#                     params={"source": "scheduled", "source_file": file_name},
#                     timeout=60,
#                 )
#
#                 # If FastAPI returns error, show response body
#                 if response.status_code >= 400:
#                     print("\n❌ FastAPI rejected file:", file_name)
#                     print("🔍 FastAPI Response Body:")
#                     print(response.text)
#                     print("======================================")
#                     response.raise_for_status()
#
#                 print(f"✅ Predicted {len(chunk)} rows for {file_name}")
#
#         except Exception as e:
#             print("\n❌ Unexpected error while processing:", file_name)
#             print("Error details:", str(e))
#             print("======================================")
#             raise
#
#
# # -------------------------------------------------------------
# # BUILD DAG
# # -------------------------------------------------------------
# def build_dag():
#     default_args = {"owner": "airflow", "retries": 0}
#
#     with DAG(
#         "prediction_dag",
#         default_args=default_args,
#         description="Scheduled prediction pipeline",
#         schedule_interval=timedelta(minutes=2),
#         start_date=days_ago(1),
#         catchup=False,
#         tags=["prediction"],
#     ) as dag:
#
#         check_task = PythonOperator(
#             task_id="check_for_new_data",
#             python_callable=check_for_new_data,
#             provide_context=True,
#         )
#
#         predict_task = PythonOperator(
#             task_id="make_predictions",
#             python_callable=make_predictions,
#             provide_context=True,
#         )
#
#         check_task >> predict_task
#
#     return dag
#
#
# dag = build_dag()
# airflow/dags/prediction_dag.py
# airflow/dags/prediction_dag.py
# airflow/dags/prediction_dag.py

import os
from datetime import timedelta, datetime
from pathlib import Path
from typing import List

import pandas as pd
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from database.db import Base, PredictionError

# -------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------
GOOD_DIR = Path("/opt/airflow/Data/good_data")

FASTAPI_PREDICT = os.getenv("FASTAPI_URL", "http://fastapi:8000/predict")

FASTAPI_HEALTH = FASTAPI_PREDICT.replace("/predict", "/health")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://admin:admin@db:5432/defence_db",
)

CHUNK_SIZE = 500
REQUIRED_COLS = ["Age", "Gender", "Geography", "EstimatedSalary"]

# -------------------------------------------------------------
# DATABASE SETUP
# -------------------------------------------------------------
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base.metadata.create_all(bind=engine)


# -------------------------------------------------------------
# HELPER: Log errors
# -------------------------------------------------------------
def log_prediction_error(file_name: str, error_type: str, message: str):
    """Store prediction error in database."""
    db = SessionLocal()
    try:
        entry = PredictionError(
            timestamp=datetime.utcnow(),
            file_name=file_name,
            error_type=error_type,
            error_message=message,
        )
        db.add(entry)
        db.commit()
    except Exception as e:
        print(f"⚠️ Failed to log error: {e}")
        db.rollback()
    finally:
        db.close()


# -------------------------------------------------------------
# HELPER: Get processed files
# -------------------------------------------------------------
def _get_processed_files():
    """Query DB for already-processed files (source='scheduled')."""
    try:
        df = pd.read_sql(
            "SELECT DISTINCT source_file FROM predictions WHERE source = 'scheduled'",
            engine
        )
        processed = set(df["source_file"].dropna().tolist())
        return processed
    except Exception as e:
        print(f"⚠️ Could not query DB: {e}")
        return set()


# -------------------------------------------------------------
# DAG CONFIG
# -------------------------------------------------------------
default_args = {
    "owner": "team",
    "retries": 0,
    "depends_on_past": False,
}

dag = DAG(
    dag_id="prediction_dag",
    default_args=default_args,
    description="Scheduled prediction pipeline - runs every 2 minutes",
    schedule_interval=timedelta(minutes=2),
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["prediction", "ml", "scheduled"],
)


# -------------------------------------------------------------
# TASK 1: CHECK FOR NEW DATA
# -------------------------------------------------------------
@task(dag=dag)
def check_for_new_data() -> List[str]:
    """
    Check for new files in good_data folder.
    Skip entire DAG if no new files found.
    """

    print(f"\n{'=' * 60}")
    print("TASK 1: Checking for new data")
    print(f"{'=' * 60}\n")

    # ============================================
    # STEP 1: FastAPI Health Check
    # ============================================
    try:
        print(f"🏥 Checking FastAPI health: {FASTAPI_HEALTH}")
        response = requests.get(FASTAPI_HEALTH, timeout=5)

        if response.status_code != 200:
            print(f"❌ FastAPI unhealthy (HTTP {response.status_code})")
            print(f"   Skipping DAG run")
            raise AirflowSkipException("FastAPI unhealthy - skipping DAG")

        print(f"✅ FastAPI is healthy\n")

    except requests.exceptions.RequestException as e:
        print(f"❌ FastAPI unreachable: {e}")
        print(f"   Skipping DAG run")
        raise AirflowSkipException("FastAPI unreachable - skipping DAG")

    # ============================================
    # STEP 2: Check if good_data folder exists
    # ============================================
    if not GOOD_DIR.exists():
        print(f"📂 Creating good_data folder: {GOOD_DIR}")
        GOOD_DIR.mkdir(parents=True, exist_ok=True)

    # ============================================
    # STEP 3: Find all CSV files
    # ============================================
    all_files = [f.name for f in GOOD_DIR.glob("*.csv") if f.is_file()]

    print(f"📊 Files in good_data folder: {len(all_files)}")

    if not all_files:
        print(f"⚠️ No CSV files found in {GOOD_DIR}")
        print(f"   Waiting for ingestion DAG to create files...")
        print(f"   Skipping DAG run\n")
        raise AirflowSkipException("No files in good_data - skipping DAG")

    # ============================================
    # STEP 4: Get already-processed files
    # ============================================
    processed_files = _get_processed_files()
    print(f"📋 Already processed files: {len(processed_files)}")

    # ============================================
    # STEP 5: Find NEW files (not yet processed)
    # ============================================
    new_files = [f for f in all_files if f not in processed_files]

    if not new_files:
        print(f"\n✅ All {len(all_files)} file(s) already processed:")
        for f in all_files:
            print(f"   - {f} (✓ already processed)")
        print(f"\n   Waiting for new data from ingestion DAG...")
        print(f"   Skipping DAG run\n")
        raise AirflowSkipException("No new files to process - skipping DAG")

    # ============================================
    # STEP 6: Return new files to process
    # ============================================
    print(f"\n✅ Found {len(new_files)} NEW file(s) to process:")
    for idx, f in enumerate(new_files, 1):
        print(f"   {idx}. {f}")

    print(f"\n{'=' * 60}\n")

    return new_files


# -------------------------------------------------------------
# TASK 2: MAKE PREDICTIONS
# -------------------------------------------------------------
@task(dag=dag)
def make_predictions(new_files: List[str]):
    """
    Process new files and send to FastAPI for predictions.
    """

    # This check should never trigger because check_for_new_data
    # already validates new_files, but keep it as a safety measure
    if not new_files:
        print("⚠️ No files received from previous task")
        return

    print(f"\n{'=' * 60}")
    print(f"TASK 2: Making predictions")
    print(f"{'=' * 60}\n")

    files_processed = 0
    files_failed = 0
    total_rows = 0

    for idx, file_name in enumerate(new_files, 1):
        file_path = GOOD_DIR / file_name

        print(f"\n[{idx}/{len(new_files)}] 📄 {file_name}")
        print("-" * 60)

        file_failed = False
        file_rows = 0

        try:
            # Validate file exists
            if not file_path.exists():
                msg = "File not found"
                print(f"❌ {msg}")
                log_prediction_error(file_name, "file_missing", msg)
                files_failed += 1
                continue

            # Validate readable
            try:
                header = pd.read_csv(file_path, nrows=1)
                if header.empty:
                    msg = "Empty file"
                    print(f"⚠️ {msg}")
                    log_prediction_error(file_name, "empty_file", msg)
                    files_failed += 1
                    continue
            except Exception as e:
                msg = f"Cannot read file: {e}"
                print(f"❌ {msg}")
                log_prediction_error(file_name, "read_error", msg)
                files_failed += 1
                continue

            # Process in chunks
            chunk_num = 0

            for chunk in pd.read_csv(file_path, chunksize=CHUNK_SIZE):

                if chunk.empty:
                    continue

                chunk_num += 1

                # Validate columns
                missing = set(REQUIRED_COLS) - set(chunk.columns)
                if missing:
                    msg = f"Missing columns: {missing}"
                    print(f"  ❌ {msg}")
                    log_prediction_error(file_name, "column_error", msg)
                    file_failed = True
                    break

                # Clean data
                chunk = chunk.replace([float("inf"), float("-inf")], None)
                chunk = chunk.where(pd.notnull(chunk), None)

                payload = chunk.to_dict(orient="records")

                print(f"  📤 Chunk {chunk_num}: {len(chunk)} rows")

                # Call API
                try:
                    response = requests.post(
                        FASTAPI_PREDICT,
                        json=payload,
                        params={"source": "scheduled", "source_file": file_name},
                        timeout=120,
                    )

                    if response.status_code >= 400:
                        msg = f"API error: {response.text[:200]}"
                        print(f"  ❌ {msg}")
                        log_prediction_error(file_name, "api_error", msg)
                        file_failed = True
                        break

                    file_rows += len(chunk)
                    print(f"  ✅ Predicted")

                except Exception as e:
                    msg = f"Request failed: {e}"
                    print(f"  ❌ {msg}")
                    log_prediction_error(file_name, "request_fail", msg)
                    file_failed = True
                    break

            # File summary
            if file_failed:
                print(f"\n⚠️ FAILED: {file_name}")
                files_failed += 1
            else:
                print(f"\n✅ SUCCESS: {file_name} ({file_rows} rows)")
                log_prediction_error(file_name, "prediction_success", f"Predicted {file_rows} rows")
                files_processed += 1
                total_rows += file_rows

        except Exception as e:
            print(f"\n❌ ERROR: {e}")
            log_prediction_error(file_name, "unexpected_error", str(e))
            files_failed += 1

    # Summary
    print(f"\n{'=' * 60}")
    print("📊 SUMMARY")
    print(f"{'=' * 60}")
    print(f"Files: {len(new_files)}")
    print(f"✅ Success: {files_processed}")
    print(f"❌ Failed: {files_failed}")
    print(f"📈 Rows: {total_rows}")
    print(f"{'=' * 60}\n")


# -------------------------------------------------------------
# DEPENDENCIES
# -------------------------------------------------------------
new_files = check_for_new_data()
make_predictions(new_files)