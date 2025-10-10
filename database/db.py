import os
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, Float, String, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Use environment variable DATABASE_URL (Docker friendly)
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://admin:admin@db:5432/defence_db"
)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()


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
    created_at = Column(TIMESTAMP, default=datetime.now)  # function, not value
