                                                   

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





