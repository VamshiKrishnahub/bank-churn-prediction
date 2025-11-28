# import os
# from datetime import datetime
# from sqlalchemy import (
#     Column,
#     Float,
#     ForeignKey,
#     Integer,
#     String,
#     TIMESTAMP,
#     create_engine,
# )
# from sqlalchemy.ext.declarative import declarative_base
# from sqlalchemy.orm import sessionmaker

# DATABASE_URL = os.getenv(
#     "DATABASE_URL",
#     "postgresql+psycopg2://admin:admin@db:5432/defence_db"
# )

# engine = create_engine(DATABASE_URL)
# SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
# Base = declarative_base()


# class Prediction(Base):
#     __tablename__ = "predictions"

#     id = Column(Integer, primary_key=True, index=True)
#     credit_score = Column(Float)
#     geography = Column(String)
#     gender = Column(String)
#     age = Column(Float)
#     tenure = Column(Float)
#     balance = Column(Float)
#     num_of_products = Column(Integer)
#     has_cr_card = Column(Integer)
#     is_active_member = Column(Integer)
#     estimated_salary = Column(Float)
#     prediction = Column(Integer)
#     source = Column(String, default="webapp")
#     source_file = Column(String, nullable=True)
#     created_at = Column(TIMESTAMP, default=datetime.utcnow)


# class IngestionStatistic(Base):
#     __tablename__ = "ingestion_statistics"

#     id = Column(Integer, primary_key=True, index=True)
#     file_name = Column(String, nullable=False)
#     total_rows = Column(Integer, nullable=False)
#     valid_rows = Column(Integer, nullable=False)
#     invalid_rows = Column(Integer, nullable=False)
#     criticality = Column(String, nullable=False)
#     report_path = Column(String, nullable=True)
#     created_at = Column(TIMESTAMP, default=datetime.utcnow, nullable=False)


# class DataQualityIssue(Base):
#     __tablename__ = "data_quality_issues"

#     id = Column(Integer, primary_key=True, index=True)
#     ingestion_id = Column(Integer, ForeignKey("ingestion_statistics.id"))
#     error_type = Column(String, nullable=False)
#     occurrences = Column(Integer, nullable=False)
#     criticality = Column(String, nullable=False)
#     created_at = Column(TIMESTAMP, default=datetime.utcnow, nullable=False)

import os
from datetime import datetime
from sqlalchemy import (
    Column,
    Float,
    ForeignKey,
    Integer,
    String,
    TIMESTAMP,
    Text,
    create_engine,
)
from sqlalchemy.orm import declarative_base, sessionmaker


# -------------------------------------------------------------
# DATABASE URL
# -------------------------------------------------------------
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://admin:admin@db:5432/defence_db",
)

# -------------------------------------------------------------
# ENGINE, SESSION, BASE
# -------------------------------------------------------------
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    future=True,
)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
)

Base = declarative_base()


# -------------------------------------------------------------
# TABLE: Predictions  (Used by prediction DAG)
# -------------------------------------------------------------
class Prediction(Base):
    __tablename__ = "predictions"

    id = Column(Integer, primary_key=True, index=True)

    credit_score = Column(Float)
    geography = Column(String)
    gender = Column(String)
    age = Column(Float)
    tenure = Column(Float)
    balance = Column(Float)
    num_of_products = Column(Integer)
    has_cr_card = Column(Integer)
    is_active_member = Column(Integer)
    estimated_salary = Column(Float)

    prediction = Column(Integer)
    source = Column(String, default="webapp")

    source_file = Column(String, nullable=True)
    created_at = Column(TIMESTAMP, default=datetime.utcnow)


# -------------------------------------------------------------
# TABLE: Ingestion Statistics  (Used by ingestion DAG)
# -------------------------------------------------------------
class IngestionStatistic(Base):
    __tablename__ = "ingestion_statistics"

    id = Column(Integer, primary_key=True, index=True)
    file_name = Column(String, nullable=False)

    total_rows = Column(Integer, nullable=False)
    valid_rows = Column(Integer, nullable=False)
    invalid_rows = Column(Integer, nullable=False)

    criticality = Column(String, nullable=False)
    report_path = Column(String, nullable=True)

    created_at = Column(TIMESTAMP, default=datetime.utcnow, nullable=False)


# -------------------------------------------------------------
# TABLE: Data Quality Issue  (Optional)
# -------------------------------------------------------------
class DataQualityIssue(Base):
    __tablename__ = "data_quality_issues"

    id = Column(Integer, primary_key=True, index=True)

    ingestion_id = Column(Integer, ForeignKey("ingestion_statistics.id"))
    error_type = Column(String, nullable=False)
    occurrences = Column(Integer, nullable=False)
    criticality = Column(String, nullable=False)

    created_at = Column(TIMESTAMP, default=datetime.utcnow, nullable=False)


# -------------------------------------------------------------
# NEW TABLE: Prediction Errors (Used by prediction DAG)
# -------------------------------------------------------------
class PredictionError(Base):
    __tablename__ = "prediction_errors"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(TIMESTAMP, default=datetime.utcnow, nullable=False)

    file_name = Column(String(255), nullable=False)
    error_type = Column(String(50), nullable=False)   # e.g., column_error, api_error
    error_message = Column(Text, nullable=False)      # full error text


# -------------------------------------------------------------
# UTILITY: Create all tables if not exist
# -------------------------------------------------------------
def init_db():
    Base.metadata.create_all(bind=engine)
