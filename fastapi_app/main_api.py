from fastapi import FastAPI
from pydantic import BaseModel
import pandas as pd
from churn_model import preprocess_and_predict

app = FastAPI(title="Bank Churn Prediction API")


# Define expected input
class CustomerFeatures(BaseModel):
    CreditScore: float
    Geography: str
    Gender: str
    Age: int
    Tenure: int
    Balance: float
    NumOfProducts: int
    HasCrCard: int
    IsActiveMember: int
    EstimatedSalary: float


@app.post("/predict")
def predict_churn(customer: CustomerFeatures):
    # Convert input to DataFrame
    input_df = pd.DataFrame([customer.dict()])

    # Get prediction
    prediction = preprocess_and_predict(input_df)

    # Convert 0/1 to human-readable
    result = "Will Churn" if prediction[0] == 1 else "Will Not Churn"
    return {"CustomerPrediction": result}
