# airflow/dags/prediction_dag.py
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd
import requests
from sqlalchemy import create_engine
import json

# -----------------------
# Config
# -----------------------
GOOD_DIR = '/opt/airflow/Data/good_data'
FASTAPI_URL = os.environ.get('FASTAPI_URL', 'http://fastapi:8000/predict')
DATABASE_URL = os.environ.get('DATABASE_URL', 'postgresql+psycopg2://admin:admin@db:5432/defence_db')

processed_files = set()  # Track already processed files
CHUNK_SIZE = 500  # Rows per chunk

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'prediction_dag',
    default_args=default_args,
    description='Prediction DAG with chunked processing',
    schedule_interval=timedelta(minutes=2),
    start_date=datetime(2025, 10, 10),
    catchup=False,
)

# -----------------------
# Tasks
# -----------------------
def check_for_new_data(**kwargs):
    files = [f for f in os.listdir(GOOD_DIR) if f.endswith('.csv')]
    new_files = [f for f in files if f not in processed_files]
    if not new_files:
        print("No new files, skipping DAG run")
        return None
    kwargs['ti'].xcom_push(key='new_files', value=new_files)
    print(f"New files to predict: {new_files}")


def make_predictions(**kwargs):
    ti = kwargs['ti']
    files = ti.xcom_pull(key='new_files', task_ids='check_for_new_data')
    if not files:
        print("No files to predict")
        return

    engine = create_engine(DATABASE_URL)

    for file in files:
        file_path = os.path.join(GOOD_DIR, file)
        print(f"Processing file: {file_path}")

        for chunk in pd.read_csv(file_path, chunksize=CHUNK_SIZE):
            payload = chunk.to_dict(orient='records')

            try:
                response = requests.post(FASTAPI_URL, json=payload, timeout=60)
                preds = response.json()
            except Exception as e:
                print(f"Error calling FastAPI: {e}")
                preds = [{} for _ in range(len(chunk))]

            # Ensure preds is a list
            if isinstance(preds, dict) and "predictions" in preds:
                preds = preds["predictions"]
            elif isinstance(preds, dict):
                preds = [preds]  # wrap single dict into a list

            if not isinstance(preds, list):
                preds = [preds]

            # Ensure predictions list matches chunk length
            if len(preds) != len(chunk):
                print(f"Warning: Prediction count {len(preds)} does not match chunk size {len(chunk)}")
                preds += [{} for _ in range(len(chunk) - len(preds))]

            # Convert all predictions to JSON string
            prediction_list = [json.dumps(p) if isinstance(p, dict) else str(p) for p in preds]

            chunk['prediction'] = prediction_list

            try:
                chunk.to_sql('predictions', engine, if_exists='append', index=False)
                print(f"Saved chunk of {len(chunk)} rows from {file}")
            except Exception as e:
                print(f"Error saving to DB: {e}")

        processed_files.add(file)
        print(f"Completed file: {file}")




# -----------------------
# Define DAG tasks
# -----------------------
task_check = PythonOperator(
    task_id='check_for_new_data',
    python_callable=check_for_new_data,
    provide_context=True,
    dag=dag,
)

task_predict = PythonOperator(
    task_id='make_predictions',
    python_callable=make_predictions,
    provide_context=True,
    dag=dag,
)

task_check >> task_predict
