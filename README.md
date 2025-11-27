                                                  

  **Bank Churn Prediction**
This project predicts whether a bank customer is likely to churn (leave the bank) using a Random Forest Classifier. It includes a data ingestion pipeline, model training, FastAPI deployment, and Docker support.
GitHub link: https://github.com/VamshiKrishnahub/bank-churn-prediction



**1. Project Overview**
   Customer churn is a significant issue for banks. Predicting churn allows banks to retain valuable customers by offering personalized incentives or services. This project provides an end-to-end pipeline from      data ingestion to deployment:
                       1.	Data ingestion and preprocessing
                       2.	Model training (Random Forest Classifier)
                       3.	Prediction API using FastAPI
                       4.	Deployment-ready Docker setup

**2. Repo Structure**
defence/
│
├─ airflow/
│  ├─ dags/
│  │  ├─ data_ingestion_dag.py        
│  │  └─ prediction_dag.py            
│
├─ great_expectations/
│  └─ placeholder.txt                
│
├─ config/
│  └─ settings.py                    
│
├─ Data/
│  ├─ raw/                           
│  ├─ data_gen_split.py               
│
├─ database/
│  ├─ db.py                           
│  └─ db.sql                          
│
├─ fastapi_app/
│  ├─ churn_model.pkl                 
│  ├─ churn_model.py                  
│  ├─ Gender_encoder.pkl              
│  ├─ Geography_encoder.pkl           
│  ├─ main_api.py                     
│  ├─ Dockerfile.py                   
│  └─ requirements.txt                
│
├─ model/
│  ├─ churn_model.pkl                 
│  ├─ Gender_encoder.pkl
│  └─ scaler.pkl                       
│
└─ README.md

**3. Project Workflow**
   Step 1: Data Ingestion
         •	Airflow DAG (data_ingestion_dag.py) automates loading raw data into the pipeline.
         •	Data is stored in the Data/raw folder.
   Step 2: Data Preprocessing
         •	Split data for training/testing (data_gen_split.py).
         •	Encode categorical variables (Gender_encoder.pkl, Geography_encoder.pkl).
         •	Scale numerical features (scaler.pkl).
   Step 3: Model Training
         •	Random Forest Classifier trains on processed data.
         •	Model saved as churn_model.pkl for later use in the API.
         •	Feature importance can be analyzed to understand churn factors.
   Step 4: Model Deployment
         •	FastAPI app (main_api.py) loads the trained model and encoders.
         •	Provides REST API endpoints for churn prediction.
         •	Dockerfile (Dockerfile.py) allows containerization for easy deployment.
   Step 5: Automated Prediction
         •	Airflow DAG (prediction_dag.py) schedules predictions on new data.
         •	Integrates seamlessly with the database for storing results.

**4. Technologies Used**
         •	Python: Data processing, modeling, and API.
         •	Scikit-learn: Random Forest Classifier and preprocessing.
         •	FastAPI: REST API for serving predictions.
         •	Airflow: Workflow automation for data ingestion and predictions.
         •	Docker: Containerization for deployment.
         •	Great Expectations: Data quality and validation (placeholder in repo).

**5. How to Run**
   Clone the repo:
                 git clone https://github.com/VamshiKrishnahub/bank-churn-prediction.git
                 cd defence
   Install dependencies:
                 pip install -r fastapi_app/requirements.txt
   Run the FastAPI server:
                 cd fastapi_app
                 uvicorn main_api:app --reload
    •	API endpoint example: POST /predict with JSON payload of customer features.
   Run Airflow DAGs:
    •	Start Airflow webserver and scheduler.
    •	DAGs data_ingestion_dag and prediction_dag automate the pipeline.
   Optional: Docker Deployment
            cd fastapi_app
            docker build -t churn-api .
            docker run -p 8000:8000 churn-api

### Run everything together (recommended)

You can bring up the database, FastAPI backend, Streamlit UI, and Airflow services at once with Docker Compose. This works both
on GitHub Codespaces and on a local machine with Docker/Compose installed.

1. Ensure Docker is running and ports `5432`, `8000`, `8080`, and `8501` are free.
2. From the repository root, start all services:

   ```bash
   docker-compose up --build
   ```

3. Service URLs once the stack is healthy:
   - Streamlit UI: http://localhost:8501
   - FastAPI docs: http://localhost:8000/docs
   - Airflow UI: http://localhost:8080 (user/password: `admin`/`admin`)
   - PostgreSQL: localhost:5432 (user/password/db: `admin` / `admin` / `defence_db`)

4. Files watched by Airflow (for ingestion/prediction DAGs):
   - Place raw batch files in `Data/raw/`.
   - Airflow writes split outputs to `Data/good_data/` and `Data/bad_data/`.

#### Codespaces-specific notes

- Open a new terminal in your Codespace and run `docker-compose up --build`; Codespaces exposes the forwarded ports in the “Ports”
  panel.
- If you restart the Codespace, rerun `docker-compose up` to bring the services back.

#### Running locally without Docker

If you prefer to run services individually (for development/debugging):

1. Start PostgreSQL locally (or with Docker) using the same credentials as in `docker-compose.yaml` (`admin`/`admin`/`defence_db`).
2. In one terminal, run the FastAPI app:

   ```bash
   cd fastapi_app
   uvicorn main_api:app --reload --host 0.0.0.0 --port 8000
   ```

3. In another terminal, launch Streamlit:

   ```bash
   cd webapp
   streamlit run main.py
   ```

4. In a third terminal, start Airflow using the environment variables from `docker-compose.yaml` (requires Airflow installed):

   ```bash
   export AIRFLOW__CORE__EXECUTOR=SequentialExecutor
   export AIRFLOW__CORE__LOAD_EXAMPLES=false
   export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://admin:admin@localhost:5432/defence_db
   export RAW_DATA_DIR=$(pwd)/Data/raw
   export GOOD_DATA_DIR=$(pwd)/Data/good_data
   export FASTAPI_URL=http://localhost:8000/predict
   export DATABASE_URL=$AIRFLOW__DATABASE__SQL_ALCHEMY_CONN

   airflow db init
   airflow users create --username admin --password admin --role Admin --firstname Max --lastname Fry --email admin@example.com
   airflow webserver & airflow scheduler
   ```

This setup keeps all components running together so you can exercise the ingestion and prediction DAGs alongside the API and UI.

**6. Results & Insights**
    •	Random Forest model predicts churn with high accuracy.
    •	Most important features:
          o	Credit Score
          o	Age
          o	Balance
          o	Tenure
    •	Banks can target at-risk customers proactively to reduce churn.
**7. Conclusion**
    •Full end-to-end ML pipeline from ingestion → training → deployment.
    •Predictive model with explainable results.
    •Ready for integration in real-world banking environments.

## Course Project Requirements

For the “Data Science in Production” course project, see the full architecture and delivery checklist in [PROJECT_INSTRUCTIONS.md](PROJECT_INSTRUCTIONS.md).





