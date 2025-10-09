from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
from fastapi.middleware.cors import CORSMiddleware
from fastapi_app.churn_model import preprocess_and_predict
from database.db import SessionLocal, Prediction
from datetime import datetime

app = FastAPI(title="Churn Prediction API")

# Allow requests from any origin (for Streamlit frontend)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Input schema
class CustomerData(BaseModel):
    CreditScore: float
    Geography: str
    Gender: str
    Age: float
    Tenure: float
    Balance: float
    NumOfProducts: int
    HasCrCard: int
    IsActiveMember: int
    EstimatedSalary: float

@app.get("/")
def home():
    return {"message": "Welcome to the Churn Prediction API"}

@app.post("/predict")
def predict(data: CustomerData):
    try:
        # Convert input to DataFrame
        df = pd.DataFrame([data.dict()])

        # Predict
        preds = preprocess_and_predict(df)
        prediction = int(preds[0])

        # Save to DB
        db = SessionLocal()
        new_pred = Prediction(
            credit_score=data.CreditScore,
            geography=data.Geography,
            gender=data.Gender,
            age=data.Age,
            tenure=data.Tenure,
            balance=data.Balance,
            num_of_products=data.NumOfProducts,
            has_cr_card=data.HasCrCard,
            is_active_member=data.IsActiveMember,
            estimated_salary=data.EstimatedSalary,
            prediction=prediction,
            created_at=datetime.now()
        )
        db.add(new_pred)
        db.commit()
        db.close()

        return {"prediction": prediction}

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/past-predictions")
def get_past_predictions():
    try:
        db = SessionLocal()
        records = db.query(Prediction).order_by(Prediction.created_at.desc()).all()
        db.close()

        # Convert records to list of dicts
        result = []
        for r in records:
            result.append({
                "id": r.id,
                "credit_score": r.credit_score,
                "geography": r.geography,
                "gender": r.gender,
                "age": r.age,
                "tenure": r.tenure,
                "balance": r.balance,
                "num_of_products": r.num_of_products,
                "has_cr_card": r.has_cr_card,
                "is_active_member": r.is_active_member,
                "estimated_salary": r.estimated_salary,
                "prediction": r.prediction,
                "created_at": r.created_at.strftime("%Y-%m-%d %H:%M:%S")
            })

        return {"past_predictions": result}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
