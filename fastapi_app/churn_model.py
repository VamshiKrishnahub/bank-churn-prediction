# fastapi_app/churn_model.py
import joblib
import pandas as pd
import numpy as np
import os

#  Load absolute paths safely
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

model = joblib.load(os.path.join(BASE_DIR, "churn_model.pkl"))
scaler = joblib.load(os.path.join(BASE_DIR, "scaler.pkl"))
geo_enc = joblib.load(os.path.join(BASE_DIR, "Geography_encoder.pkl"))
gen_enc = joblib.load(os.path.join(BASE_DIR, "Gender_encoder.pkl"))


def safe_transform(encoder, values):
    """Handle unseen categories gracefully."""
    known_classes = set(encoder.classes_)
    transformed = []
    for v in values:
        if v in known_classes:
            transformed.append(encoder.transform([v])[0])
        else:
            transformed.append(-1)
    return np.array(transformed)


def preprocess_and_predict(input_data: pd.DataFrame):
    """
    input_data: pandas DataFrame with raw features
    returns: list of predictions
    """
    df = input_data.copy()

    #  Encode categorical features
    df["Geography"] = safe_transform(geo_enc, df["Geography"])
    df["Gender"] = safe_transform(gen_enc, df["Gender"])

    #  Define all features used during training
    feature_cols = [
        "CreditScore", "Geography", "Gender", "Age", "Tenure", "Balance",
        "NumOfProducts", "HasCrCard", "IsActiveMember", "EstimatedSalary"
    ]

    #  Reorder columns
    df = df[feature_cols]

    #  Apply scaler (it was fit on all columns during training)
    df_scaled = pd.DataFrame(scaler.transform(df), columns=feature_cols)

    #  Predict using the model
    preds = model.predict(df_scaled)
    return preds.tolist()


#  Local test
if __name__ == "__main__":
    sample = pd.DataFrame([{
        "CreditScore": 600,
        "Geography": "France",
        "Gender": "Male",
        "Age": 40,
        "Tenure": 3,
        "Balance": 60000,
        "NumOfProducts": 2,
        "HasCrCard": 1,
        "IsActiveMember": 1,
        "EstimatedSalary": 50000
    }])

    result = preprocess_and_predict(sample)
    print(" Prediction:", result)
