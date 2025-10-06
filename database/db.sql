import joblib
import pandas as pd

# Load model and encoders/scalers
model = joblib.load("fastapi_app/churn_model.pkl")
scaler = joblib.load("fastapi_app/scaler.pkl")
geo_enc = joblib.load("fastapi_app/geo_enc.pkl")
gender_enc = joblib.load("fastapi_app/gender_enc.pkl")

def preprocess_and_predict(input_data):
    """
    input_data: pandas DataFrame with raw features
    returns: predictions
    """
    df = input_data.copy()

    # Encode categorical variables
    df['Geography'] = geo_enc.transform(df['Geography'])
    df['Gender'] = gender_enc.transform(df['Gender'])

    # Scale numeric features
    numeric_cols = ['CreditScore','Age','Tenure','Balance','NumOfProducts','HasCrCard','IsActiveMember','EstimatedSalary']
    df[numeric_cols] = scaler.transform(df[numeric_cols])

    # Predict
    preds = model.predict(df)
    return preds.tolist()
