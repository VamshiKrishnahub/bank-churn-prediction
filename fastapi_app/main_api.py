from datetime import datetime
from typing import List, Optional, Union
from pathlib import Path

import pandas as pd
from fastapi import Body, Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from churn_model import preprocess_and_predict
from database.db import Base, Prediction, SessionLocal, engine


Base.metadata.create_all(bind=engine)


app = FastAPI(title="Churn Prediction API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


REPORT_DIR = Path("/opt/airflow/Data/reports")
REPORT_DIR.mkdir(parents=True, exist_ok=True)

app.mount(
    "/reports",
    StaticFiles(directory=str(REPORT_DIR), html=True),
    name="reports"
)


@app.get("/", include_in_schema=False)
def root_redirect():
    return RedirectResponse(url="/docs")


@app.get("/health")
def health_check():
    return {
        "status": "healthy",
        "service": "churn-prediction-api",
        "timestamp": datetime.utcnow().isoformat(),
    }


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


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.post("/predict")
def predict(
    data: Union[CustomerData, List[CustomerData]] = Body(..., embed=False),
    source: str = Query("webapp"),
    source_file: Optional[str] = Query(None),
    db=Depends(get_db),
):

    try:
        rows = data if isinstance(data, list) else [data]
        df = pd.DataFrame([row.dict() for row in rows])

        # Clean invalid values
        df = df.replace({float("nan"): 0, float("inf"): 0, float("-inf"): 0})

        predictions = preprocess_and_predict(df)
        timestamp = datetime.utcnow()
        saved_predictions = []

        for row, pred in zip(rows, predictions):
            pred_value = int(pred)

            entry = Prediction(
                credit_score=row.CreditScore,
                geography=row.Geography,
                gender=row.Gender,
                age=row.Age,
                tenure=row.Tenure,
                balance=row.Balance,
                num_of_products=row.NumOfProducts,
                has_cr_card=row.HasCrCard,
                is_active_member=row.IsActiveMember,
                estimated_salary=row.EstimatedSalary,
                prediction=pred_value,
                source=source,
                source_file=source_file,
                created_at=timestamp,
            )

            db.add(entry)
            saved_predictions.append({
                "prediction": pred_value,
                "prediction_label": "Will churn" if pred_value == 1 else "Will not churn",
                "source": source,
                "source_file": source_file,
            })

        db.commit()

        return saved_predictions[0] if len(saved_predictions) == 1 else {
            "predictions": saved_predictions,
            "count": len(saved_predictions),
        }

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/past-predictions")
def get_past_predictions(
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    source: str = Query("all"),
    limit: int = Query(200, ge=1, le=1000),
    db=Depends(get_db),
):

    try:
        query = db.query(Prediction)

        if start_date:
            query = query.filter(Prediction.created_at >= start_date)
        if end_date:
            query = query.filter(Prediction.created_at <= end_date)
        if source != "all":
            query = query.filter(Prediction.source == source)

        records = query.order_by(Prediction.created_at.desc()).limit(limit).all()

        data = [
            {
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
                "prediction_label": "Will churn" if r.prediction == 1 else "Will not churn",
                "created_at": r.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                "source": r.source,
                "source_file": r.source_file,
            }
            for r in records
        ]

        return {"past_predictions": data, "count": len(data)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
