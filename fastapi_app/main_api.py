# from datetime import datetime
# from typing import List, Optional, Union
#
# import pandas as pd
# from fastapi import Body, Depends, FastAPI, HTTPException, Query
# from fastapi.middleware.cors import CORSMiddleware
# from pydantic import BaseModel
#
# from churn_model import preprocess_and_predict  # your model logic
# from database.db import Base, Prediction, SessionLocal, engine
#
# Base.metadata.create_all(bind=engine)
#
#
# app = FastAPI(title="Churn Prediction API")
#
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )
#
# class CustomerData(BaseModel):
#     CreditScore: float
#     Geography: str
#     Gender: str
#     Age: float
#     Tenure: float
#     Balance: float
#     NumOfProducts: int
#     HasCrCard: int
#     IsActiveMember: int
#     EstimatedSalary: float
#
#
#
# def get_db():
#     db = SessionLocal()
#     try:
#         yield db
#     finally:
#         db.close()
#
# @app.get("/")
# def home():
#     return {"message": "Welcome to the Churn Prediction API"}
#
#
#
# @app.post("/predict")
# def predict(
#     data: Union[CustomerData, List[CustomerData]] = Body(..., embed=False),
#     source: str = Query("webapp"),
#     source_file: Optional[str] = Query(None),
#     db=Depends(get_db),
# ):
#     """Handle single and batch prediction requests.
#
#     The endpoint accepts either a single payload or a list of payloads and
#     returns aligned prediction results. Each prediction is stored with the
#     provided source to support monitoring and Airflow ingestion.
#     """
#
#     try:
#         rows = data if isinstance(data, list) else [data]
#         df = pd.DataFrame([row.dict() for row in rows])
#
#         preds = preprocess_and_predict(df)
#         predictions_list: List[dict] = []
#         timestamp = datetime.utcnow()
#
#         for row, pred in zip(rows, preds):
#             prediction_value = int(pred)
#
#             db_obj = Prediction(
#                 credit_score=row.CreditScore,
#                 geography=row.Geography,
#                 gender=row.Gender,
#                 age=row.Age,
#                 tenure=row.Tenure,
#                 balance=row.Balance,
#                 num_of_products=row.NumOfProducts,
#                 has_cr_card=row.HasCrCard,
#                 is_active_member=row.IsActiveMember,
#                 estimated_salary=row.EstimatedSalary,
#                 prediction=prediction_value,
#                 source=source,
#                 source_file=source_file,
#                 created_at=timestamp,
#             )
#             db.add(db_obj)
#
#             predictions_list.append(
#                 {
#                     "prediction": prediction_value,
#                     "prediction_label": "Will churn"
#                     if prediction_value == 1
#                     else "Will not churn",
#                     "source": source,
#                     "source_file": source_file,
#                 }
#             )
#
#         db.commit()
#
#         if len(predictions_list) == 1:
#             return predictions_list[0]
#
#         return {"predictions": predictions_list, "count": len(predictions_list)}
#
#     except Exception as e:
#         db.rollback()
#         import traceback
#
#         error_detail = f"{str(e)}\n{traceback.format_exc()}"
#         print(f"Prediction error: {error_detail}")
#         raise HTTPException(status_code=400, detail=str(e))
#
#
# @app.get("/past-predictions")
# def get_past_predictions(
#     start_date: Optional[datetime] = Query(None, description="Filter from this timestamp"),
#     end_date: Optional[datetime] = Query(None, description="Filter until this timestamp"),
#     source: str = Query("all", description="webapp | scheduled | all"),
#     limit: int = Query(200, ge=1, le=1000, description="Maximum number of rows to return"),
#     db=Depends(get_db),
# ):
#     try:
#         query = db.query(Prediction)
#
#         if start_date:
#             query = query.filter(Prediction.created_at >= start_date)
#         if end_date:
#             query = query.filter(Prediction.created_at <= end_date)
#         if source != "all":
#             query = query.filter(Prediction.source == source)
#
#         records = (
#             query.order_by(Prediction.created_at.desc()).limit(limit).all()
#         )
#
#         result = [
#             {
#                 "id": r.id,
#                 "credit_score": r.credit_score,
#                 "geography": r.geography,
#                 "gender": r.gender,
#                 "age": r.age,
#                 "tenure": r.tenure,
#                 "balance": r.balance,
#                 "num_of_products": r.num_of_products,
#                 "has_cr_card": r.has_cr_card,
#                 "is_active_member": r.is_active_member,
#                 "estimated_salary": r.estimated_salary,
#                 "prediction": r.prediction,
#                 "prediction_label": "Will churn" if r.prediction == 1 else "Will not churn",
#                 "created_at": r.created_at.strftime("%Y-%m-%d %H:%M:%S"),
#                 "source": r.source,
#                 "source_file": r.source_file,
#             }
#             for r in records
#         ]
#         return {"past_predictions": result, "count": len(result)}
#
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))

from datetime import datetime
from typing import List, Optional, Union

import pandas as pd
from fastapi import Body, Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
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

# ---------------------------------------------------------------------
# ⭐ Auto redirect root URL "/" → "/docs"
# ---------------------------------------------------------------------
@app.get("/", include_in_schema=False)
def root_redirect():
    return RedirectResponse(url="/docs")


# ---------------------------------------------------------------------
# Pydantic Model
# ---------------------------------------------------------------------
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


# ---------------------------------------------------------------------
# DB dependency
# ---------------------------------------------------------------------
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ---------------------------------------------------------------------
# Prediction Endpoint (Single + Batch)
# ---------------------------------------------------------------------
@app.post("/predict")
def predict(
    data: Union[CustomerData, List[CustomerData]] = Body(..., embed=False),
    source: str = Query("webapp"),
    source_file: Optional[str] = Query(None),
    db=Depends(get_db),
):
    """Single or batch predictions"""
    try:
        rows = data if isinstance(data, list) else [data]
        df = pd.DataFrame([row.dict() for row in rows])

        # Fix NaN/infinity before prediction
        df = df.replace({float("nan"): 0, float("inf"): 0, float("-inf"): 0})

        preds = preprocess_and_predict(df)

        timestamp = datetime.utcnow()
        predictions_list = []

        for row, pred in zip(rows, preds):
            prediction_value = int(pred)

            db_obj = Prediction(
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
                prediction=prediction_value,
                source=source,
                source_file=source_file,
                created_at=timestamp,
            )
            db.add(db_obj)

            predictions_list.append(
                {
                    "prediction": prediction_value,
                    "prediction_label": "Will churn" if prediction_value == 1 else "Will not churn",
                    "source": source,
                    "source_file": source_file,
                }
            )

        db.commit()

        if len(predictions_list) == 1:
            return predictions_list[0]

        return {"predictions": predictions_list, "count": len(predictions_list)}

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))


# ---------------------------------------------------------------------
# Past Predictions Endpoint
# ---------------------------------------------------------------------
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

        result = [
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

        return {"past_predictions": result, "count": len(result)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))