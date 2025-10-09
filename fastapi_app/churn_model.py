import joblib
import pandas as pd
import numpy as np
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Load model and encoders
model = joblib.load(os.path.join(BASE_DIR, "churn_model.pkl"))
scaler = joblib.load(os.path.join(BASE_DIR, "scaler.pkl"))
geo_enc = joblib.load(os.path.join(BASE_DIR, "Geography_encoder.pkl"))
gen_enc = joblib.load(os.path.join(BASE_DIR, "Gender_encoder.pkl"))

def safe_transform(encoder, values):
    """Transform unseen categories as -1"""
    known_classes = set(encoder.classes_)
    transformed = []
    for v in values:
        if v in known_classes:
            transformed.append(encoder.transform([v])[0])
        else:
            transformed.append(-1)
    return np.array(transformed)

# def preprocess_and_predict(input_data: pd.DataFrame):
#     """
#     input_data: pandas DataFrame
#     returns: list of predictions
#     """
#     df = input_data.copy()
#     df["Geography"] = safe_transform(geo_enc, df["Geography"])
#     df["Gender"] = safe_transform(gen_enc, df["Gender"])
#
#     feature_cols = [
#         "CreditScore", "Geography", "Gender", "Age", "Tenure",
#         "Balance", "NumOfProducts", "HasCrCard", "IsActiveMember", "EstimatedSalary"
#     ]
#
#     df = df[feature_cols]
#
#     # Keep feature names for scaler
#     df_scaled = pd.DataFrame(scaler.transform(df), columns=feature_cols)
#
#     # Convert to numpy for model
#     preds = model.predict(df_scaled.to_numpy())
#
#     return preds.tolist()

def preprocess_and_predict(input_data: pd.DataFrame):
    try:
        df = input_data.copy()

        # Make sure all expected columns exist
        expected_cols = [
            "CreditScore","Geography","Gender","Age","Tenure",
            "Balance","NumOfProducts","HasCrCard","IsActiveMember","EstimatedSalary"
        ]
        for col in expected_cols:
            if col not in df.columns:
                df[col] = 0  # or some default

        # Encode categorical
        df["Geography"] = safe_transform(geo_enc, df["Geography"])
        df["Gender"] = safe_transform(gen_enc, df["Gender"])

        # Reorder
        df = df[expected_cols]

        # Scale
        df_scaled = pd.DataFrame(scaler.transform(df), columns=expected_cols)

        # Predict
        preds = model.predict(df_scaled.to_numpy())
        return preds.tolist()
    except Exception as e:
        # Return readable error instead of crashing
        raise ValueError(f"Prediction failed: {str(e)}")
