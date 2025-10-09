import streamlit as st
import pandas as pd
import requests
import os
from datetime import datetime

# API URL - Use environment variable for Docker, fallback to localhost for local dev
API_BASE_URL = os.getenv("FASTAPI_URL", "http://localhost:8000")
API_URL = f"{API_BASE_URL}/predict"
PAST_PREDICTIONS_URL = f"{API_BASE_URL}/past-predictions"

st.set_page_config(page_title="Churn Prediction", layout="wide")
st.title("Churn Prediction Webapp")

menu = ["Single Prediction", "Batch Prediction", "Past Predictions"]
choice = st.sidebar.selectbox("Menu", menu)

# ---------------- SINGLE PREDICTION ---------------- #
if choice == "Single Prediction":
    st.header("Make a Single Prediction")

    with st.form(key="single_form"):
        CreditScore = st.number_input("Credit Score", min_value=300, max_value=900, value=600)
        Geography = st.selectbox("Geography", ["France", "Spain", "Germany"])
        Gender = st.selectbox("Gender", ["Male", "Female"])
        Age = st.number_input("Age", min_value=18, max_value=100, value=40)
        Tenure = st.number_input("Tenure", min_value=0, max_value=10, value=3)
        Balance = st.number_input("Balance", min_value=0.0, value=60000.0)
        NumOfProducts = st.number_input("Number of Products", min_value=1, max_value=4, value=2)
        HasCrCard = st.selectbox("Has Credit Card", [0, 1], index=1)
        IsActiveMember = st.selectbox("Is Active Member", [0, 1], index=1)
        EstimatedSalary = st.number_input("Estimated Salary", min_value=0.0, value=50000.0)

        submit_button = st.form_submit_button("Predict")

    if submit_button:
        payload = {
            "CreditScore": CreditScore,
            "Geography": Geography,
            "Gender": Gender,
            "Age": Age,
            "Tenure": Tenure,
            "Balance": Balance,
            "NumOfProducts": NumOfProducts,
            "HasCrCard": HasCrCard,
            "IsActiveMember": IsActiveMember,
            "EstimatedSalary": EstimatedSalary
        }

        try:
            response = requests.post(API_URL, json=payload)
            result = response.json()
            st.success(f"Prediction: {result['prediction']}")
        except Exception as e:
            st.error(f"API call failed: {e}")

# ---------------- BATCH PREDICTION ---------------- #
elif choice == "Batch Prediction":
    st.header("Upload CSV for Batch Predictions")
    uploaded_file = st.file_uploader("Choose a CSV file", type="csv")
    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file)
        st.write("Uploaded Data:")
        st.dataframe(df)

        if st.button("Predict All"):
            predictions = []
            for _, row in df.iterrows():
                payload = row.to_dict()
                response = requests.post(API_URL, json=payload)
                predictions.append(response.json()["prediction"])
            df["Prediction"] = predictions
            st.success("Predictions Added:")
            st.dataframe(df)

# ---------------- PAST PREDICTIONS ---------------- #
elif choice == "Past Predictions":
    st.header("View Past Predictions from DB")

    if st.button("Fetch Past Predictions"):
        try:
            response = requests.get(PAST_PREDICTIONS_URL)
            data = response.json()

            if "past_predictions" in data:
                df = pd.DataFrame(data["past_predictions"])
                st.dataframe(df)
            else:
                st.warning("No past predictions found.")
        except Exception as e:
            st.error(f"Failed to fetch data: {e}")