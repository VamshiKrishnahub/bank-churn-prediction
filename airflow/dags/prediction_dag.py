from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import os
import pandas as pd
import requests
from sqlalchemy import create_engine
import json
from airflow.exceptions import AirflowSkipException


GOOD_DIR = '/opt/airflow/Data/good_data'
FASTAPI_URL = os.environ.get('FASTAPI_URL', 'http://fastapi:8000/predict')
DATABASE_URL = os.environ.get('DATABASE_URL', 'postgresql+psycopg2://admin:admin@db:5432/defence_db')
CHUNK_SIZE = 500

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'prediction_dag',
    default_args=default_args,
    description='Prediction DAG with persistent skip logic',
    schedule_interval=timedelta(minutes=2),
    start_date=datetime(2025, 10, 10),
    catchup=False,
)


def get_processed_files():
    engine = create_engine(DATABASE_URL)
    try:
        df = pd.read_sql("SELECT DISTINCT source_file FROM predictions", engine)
        return set(df['source_file'].tolist())
    except Exception:
        return set()

def check_for_new_data(**kwargs):
    files = [f for f in os.listdir(GOOD_DIR) if f.endswith('.csv')]
    processed_files = get_processed_files()
    new_files = [f for f in files if f not in processed_files]

    if not new_files:
        print("No new files found — skipping prediction.")
        return 'skip_predictions'

    kwargs['ti'].xcom_push(key='new_files', value=new_files)
    print(f"New files to predict: {new_files}")
    return 'make_predictions'


def make_predictions(**kwargs):
    ti = kwargs['ti']
    files = ti.xcom_pull(key='new_files', task_ids='check_for_new_data')
    if not files:
        raise AirflowSkipException("No files to predict")  # safety

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


            if isinstance(preds, dict) and "predictions" in preds:
                preds = preds["predictions"]
            elif isinstance(preds, dict):
                preds = [preds]
            if not isinstance(preds, list):
                preds = [preds]
            if len(preds) != len(chunk):
                preds += [{} for _ in range(len(chunk) - len(preds))]


            chunk['prediction'] = [json.dumps(p) if isinstance(p, dict) else str(p) for p in preds]
            chunk['source_file'] = file  # track origin

            try:
                chunk.to_sql('predictions', engine, if_exists='append', index=False)
                print(f"Saved chunk of {len(chunk)} rows from {file}")
            except Exception as e:
                print(f"Error saving to DB: {e}")



skip_predictions = DummyOperator(task_id='skip_predictions', dag=dag)


branch_check = BranchPythonOperator(
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

branch_check >> [task_predict, skip_predictions]
