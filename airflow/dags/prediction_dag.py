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


def log_prediction_error(file_name: str, error_type: str, message: str):

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


def _get_processed_files():

    try:
        df = pd.read_sql(
            "SELECT DISTINCT source_file FROM predictions WHERE source = 'scheduled'",
            engine
        )
        processed = set(df["source_file"].dropna().tolist())
        return processed
    except Exception as e:
        print(f"️ Could not query DB: {e}")
        return set()

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

@task(dag=dag)
def check_for_new_data() -> List[str]:

    print(f"\n{'=' * 60}")
    print("TASK 1: Checking for new data")
    print(f"{'=' * 60}\n")

    try:
        print(f" Checking FastAPI health: {FASTAPI_HEALTH}")
        response = requests.get(FASTAPI_HEALTH, timeout=5)

        if response.status_code != 200:
            print(f" FastAPI unhealthy (HTTP {response.status_code})")
            print(f"   Skipping DAG run")
            raise AirflowSkipException("FastAPI unhealthy - skipping DAG")

        print(f" FastAPI is healthy\n")

    except requests.exceptions.RequestException as e:
        print(f" FastAPI unreachable: {e}")
        print(f"   Skipping DAG run")
        raise AirflowSkipException("FastAPI unreachable - skipping DAG")

    if not GOOD_DIR.exists():
        print(f"📂 Creating good_data folder: {GOOD_DIR}")
        GOOD_DIR.mkdir(parents=True, exist_ok=True)

    all_files = [f.name for f in GOOD_DIR.glob("*.csv") if f.is_file()]

    print(f" Files in good_data folder: {len(all_files)}")

    if not all_files:
        print(f"️ No CSV files found in {GOOD_DIR}")
        print(f"   Waiting for ingestion DAG to create files...")
        print(f"   Skipping DAG run\n")
        raise AirflowSkipException("No files in good_data - skipping DAG")

    processed_files = _get_processed_files()
    print(f" Already processed files: {len(processed_files)}")

    new_files = [f for f in all_files if f not in processed_files]

    if not new_files:
        print(f"\n All {len(all_files)} file(s) already processed:")
        for f in all_files:
            print(f"   - {f} (✓ already processed)")
        print(f"\n   Waiting for new data from ingestion DAG...")
        print(f"   Skipping DAG run\n")
        raise AirflowSkipException("No new files to process - skipping DAG")

    print(f"\n Found {len(new_files)} NEW file(s) to process:")
    for idx, f in enumerate(new_files, 1):
        print(f"   {idx}. {f}")

    print(f"\n{'=' * 60}\n")

    return new_files


@task(dag=dag)
def make_predictions(new_files: List[str]):

    if not new_files:
        print("️ No files received from previous task")
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
                print(f" {msg}")
                log_prediction_error(file_name, "file_missing", msg)
                files_failed += 1
                continue

            # Validate readable
            try:
                header = pd.read_csv(file_path, nrows=1)
                if header.empty:
                    msg = "Empty file"
                    print(f"️ {msg}")
                    log_prediction_error(file_name, "empty_file", msg)
                    files_failed += 1
                    continue
            except Exception as e:
                msg = f"Cannot read file: {e}"
                print(f" {msg}")
                log_prediction_error(file_name, "read_error", msg)
                files_failed += 1
                continue

            chunk_num = 0

            for chunk in pd.read_csv(file_path, chunksize=CHUNK_SIZE):

                if chunk.empty:
                    continue

                chunk_num += 1

                # Validate columns
                missing = set(REQUIRED_COLS) - set(chunk.columns)
                if missing:
                    msg = f"Missing columns: {missing}"
                    print(f"   {msg}")
                    log_prediction_error(file_name, "column_error", msg)
                    file_failed = True
                    break

                # Clean data
                chunk = chunk.replace([float("inf"), float("-inf")], None)
                chunk = chunk.where(pd.notnull(chunk), None)

                payload = chunk.to_dict(orient="records")

                print(f"   Chunk {chunk_num}: {len(chunk)} rows")

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
                        print(f"   {msg}")
                        log_prediction_error(file_name, "api_error", msg)
                        file_failed = True
                        break

                    file_rows += len(chunk)
                    print(f"   Predicted")

                except Exception as e:
                    msg = f"Request failed: {e}"
                    print(f"   {msg}")
                    log_prediction_error(file_name, "request_fail", msg)
                    file_failed = True
                    break

            # File summary
            if file_failed:
                print(f"\n️ FAILED: {file_name}")
                files_failed += 1
            else:
                print(f"\n SUCCESS: {file_name} ({file_rows} rows)")
                log_prediction_error(file_name, "prediction_success", f"Predicted {file_rows} rows")
                files_processed += 1
                total_rows += file_rows

        except Exception as e:
            print(f"\n ERROR: {e}")
            log_prediction_error(file_name, "unexpected_error", str(e))
            files_failed += 1

    # Summary
    print(f"\n{'=' * 60}")
    print(" SUMMARY")
    print(f"{'=' * 60}")
    print(f"Files: {len(new_files)}")
    print(f" Success: {files_processed}")
    print(f" Failed: {files_failed}")
    print(f" Rows: {total_rows}")
    print(f"{'=' * 60}\n")

new_files = check_for_new_data()
make_predictions(new_files)