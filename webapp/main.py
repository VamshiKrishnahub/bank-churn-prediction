
import os
from datetime import datetime, time

import pandas as pd
import requests
import streamlit as st

API_BASE_URL = os.getenv("FASTAPI_URL", "http://localhost:8000")
API_URL = f"{API_BASE_URL}/predict"
PAST_PREDICTIONS_URL = f"{API_BASE_URL}/past-predictions"

st.set_page_config(page_title="Churn Prediction", layout="wide")
st.title("Churn Prediction Webapp")

menu = ["Single Prediction", "Batch Prediction", "Past Predictions"]
choice = st.sidebar.selectbox("Menu", menu)

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
            "EstimatedSalary": EstimatedSalary,
        }

        try:
            response = requests.post(API_URL, json=payload, params={"source": "webapp"})
            response.raise_for_status()
            result = response.json()

            display_row = payload.copy()
            display_row["Prediction"] = result.get("prediction_label", "")
            display_row["RawPrediction"] = result.get("prediction")

            if "prediction_label" in result:
                if result.get("prediction") == 1:
                    st.error(f"🚨 {result['prediction_label']}")
                else:
                    st.success(f"✅ {result['prediction_label']}")
            else:
                st.info(f"Prediction: {result.get('prediction')}")

            st.dataframe(pd.DataFrame([display_row]))

        except Exception as e:
            st.error(f"API call failed: {e}")


elif choice == "Batch Prediction":
    st.header("Upload CSV for Batch Predictions")
    st.caption(
        "CSV must include: CreditScore, Geography, Gender, Age, Tenure, Balance, "
        "NumOfProducts, HasCrCard, IsActiveMember, EstimatedSalary"
    )

    uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

    if uploaded_file is not None:
        df_raw = pd.read_csv(uploaded_file)
        st.subheader("Uploaded Data")
        st.dataframe(df_raw)

        required = [
            "CreditScore", "Geography", "Gender", "Age", "Tenure",
            "Balance", "NumOfProducts", "HasCrCard", "IsActiveMember", "EstimatedSalary"
        ]


        missing = [c for c in required if c not in df_raw.columns]

        if missing:
            st.error(f"Missing required columns: {missing}")
            st.stop()

        df = df_raw[required].copy()


        df = df.replace("", None)
        df = df.replace(" ", None)


        for col in ["Geography", "Gender"]:
            df[col] = df[col].astype(str).str.strip()


        num_cols = ["CreditScore", "Age", "Tenure", "Balance",
                    "NumOfProducts", "HasCrCard", "IsActiveMember",
                    "EstimatedSalary"]

        for col in num_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")


        df = df.where(pd.notnull(df), None)


        before = len(df)
        df = df.dropna(subset=required)
        after = len(df)

        if after < before:
            st.warning(f"Dropped {before - after} invalid rows before prediction.")

        st.subheader("Cleaned Data to Send to API")
        st.dataframe(df)

        if st.button("Predict All"):
            try:
                payload = df.to_dict(orient="records")

                response = requests.post(
                    API_URL,
                    json=payload,
                    params={"source": "webapp", "source_file": "webapp_upload"},
                )
                response.raise_for_status()

                results = response.json()

                if "predictions" in results:
                    preds = results["predictions"]
                else:
                    preds = [results]

                df["RawPrediction"] = [p["prediction"] for p in preds]
                df["Prediction"] = [p["prediction_label"] for p in preds]

                st.success("Batch predictions complete!")
                st.dataframe(df)

            except Exception as e:
                st.error(f"API call failed: {e}")


elif choice == "Past Predictions":
    st.header("View Past Predictions from DB")

    source_options = {
        "All": "all",
        "Webapp": "webapp",
        "Scheduled predictions": "scheduled",
    }

    selected_source = st.selectbox("Filter by Source", list(source_options.keys()))
    source_filter = source_options[selected_source]

    use_date_filter = st.checkbox("Filter by date range")

    params = {"source": source_filter}

    if use_date_filter:
        start_date = st.date_input("Start date", value=datetime.utcnow().date())
        end_date = st.date_input("End date", value=datetime.utcnow().date())

        params["start_date"] = datetime.combine(start_date, time.min).isoformat()
        params["end_date"] = datetime.combine(end_date, time.max).isoformat()

    if st.button("Fetch Past Predictions"):
        try:
            response = requests.get(PAST_PREDICTIONS_URL, params=params)
            response.raise_for_status()
            data = response.json()

            if data.get("past_predictions"):
                df = pd.DataFrame(data["past_predictions"])
                st.dataframe(df)
            else:
                st.warning("No past predictions found.")

        except Exception as e:
            st.error(f"Failed to fetch past predictions: {e}")
