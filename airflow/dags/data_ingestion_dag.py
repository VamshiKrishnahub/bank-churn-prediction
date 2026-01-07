from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowSkipException

import os
import random
import uuid
import shutil
from pathlib import Path
from typing import Dict, List

import pandas as pd
import numpy as np
import great_expectations as ge
from great_expectations.data_context import FileDataContext
from great_expectations.core.batch import RuntimeBatchRequest

from database.db import (
    Base,
    SessionLocal,
    engine,
    IngestionStatistic,
)

from send_alerts import send_teams_alert

DATA_DIR = Path("/opt/airflow/Data")

RAW_DATA_SOURCE = DATA_DIR / "raw-data"
LEGACY_RAW_DIR = DATA_DIR / "raw"
GOOD_DIR = DATA_DIR / "good_data"
BAD_DIR = DATA_DIR / "bad_data"
ARCHIVE_DIR = DATA_DIR / "archive_raw"
REPORTS_DIR = DATA_DIR / "reports"
GE_DIR = DATA_DIR / "great_expectations"

FASTAPI_REPORT_URL = "http://fastapi:8000/reports"

default_args = {
    "owner": "team",
    "depends_on_past": False,
    "retries": 0,
    "max_active_runs": 1,
}

dag = DAG(
    dag_id="data_ingestion_dag",
    default_args=default_args,
    description="Data ingestion pipeline with Great Expectations",
    schedule="*/1 * * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "data-quality"],
)


@task(dag=dag)
def read_data() -> str:
    print(f"\n{'=' * 60}")
    print("TASK 1: Reading raw data file")
    print(f"{'=' * 60}\n")

    RAW_DATA_SOURCE.mkdir(exist_ok=True)
    available = list(RAW_DATA_SOURCE.glob("*.csv"))

    if not available:
        LEGACY_RAW_DIR.mkdir(exist_ok=True)
        available = list(LEGACY_RAW_DIR.glob("*.csv"))

    if not available:
        raise AirflowSkipException("No CSV files found.")

    chosen = random.choice(available)

    print(f" Selected: {chosen.name}")
    print(f"   Total files available: {len(available)}")
    print(f"{'=' * 60}\n")

    return str(chosen)


@task(dag=dag)
def validate_data(file_path: str) -> Dict:
    """Validate data using Great Expectations with native Data Docs"""

    print(f"\n{'=' * 60}")
    print("TASK 2: Validating data with Great Expectations")
    print(f"{'=' * 60}\n")

    try:
        df = pd.read_csv(file_path)
        print(f" Loaded: {len(df)} rows")
    except Exception as exc:
        raise AirflowSkipException(f" Cannot read file: {exc}")

    if df.empty:
        raise AirflowSkipException(" Empty file")

    # Use simple GE for validation
    ge_df = ge.from_pandas(df)

    errors: List[Dict] = []
    results: List[Dict] = []
    severity_rank = {"low": 1, "medium": 2, "high": 3}
    criticality = "low"

    def serialize(obj):
        if hasattr(obj, "to_json_dict"):
            return obj.to_json_dict()
        if isinstance(obj, dict):
            return obj
        return {"raw": str(obj)}

    def check(name: str, severity: str, description: str, fn):
        nonlocal criticality
        try:
            out = fn()
        except Exception as exc:
            out = {"success": False, "error": str(exc)}

        out = serialize(out)
        results.append({
            "check": name,
            "description": description,
            "severity": severity,
            "result": out
        })

        if not out.get("success", False):
            errors.append({
                "type": name,
                "message": description,
                "severity": severity,
                "details": out
            })
            if severity_rank[severity] > severity_rank[criticality]:
                criticality = severity

    # Required columns
    required = ["Age", "Gender", "Geography", "EstimatedSalary"]

    # 1. Missing column checks
    for col in required:
        check("missing_column", "high", f"Column '{col}' must exist",
              lambda col=col: ge_df.expect_column_to_exist(col))

    if not all(c in df.columns for c in required):
        return {
            "file_path": file_path,
            "errors": errors,
            "criticality": "high",
            "valid_rows": 0,
            "invalid_rows": len(df),
            "expectations": results,
            "bad_row_indices": list(range(len(df))),
            "good_row_indices": [],
            "ge_report_url": None
        }

    # 2-10. All validation checks
    for col in required:
        check("missing_value", "medium", f"Column '{col}' cannot have nulls",
              lambda col=col: ge_df.expect_column_values_to_not_be_null(col))

    def check_age_numeric():
        age_num = pd.to_numeric(df["Age"], errors="coerce")
        non_num = age_num.isna() & df["Age"].notna()
        return {"success": int(non_num.sum()) == 0, "details": {"non_numeric_age_count": int(non_num.sum())}}

    check("value_error", "high", "Age must be numeric", check_age_numeric)
    check("out_of_range_age", "high", "Age must be 0-120",
          lambda: ge_df.expect_column_values_to_be_between("Age", 0, 120))
    check("out_of_range_income", "high", "EstimatedSalary must be 0-1,000,000",
          lambda: ge_df.expect_column_values_to_be_between("EstimatedSalary", 0, 1_000_000))
    check("categorical_error_gender", "high", "Gender must be Male/Female",
          lambda: ge_df.expect_column_values_to_be_in_set("Gender", ["Male", "Female"]))
    check("categorical_error_geography", "medium", "Geography must be France/Spain/Germany",
          lambda: ge_df.expect_column_values_to_be_in_set("Geography", ["France", "Spain", "Germany"]))

    dup_rows = df.duplicated(keep=False)
    check("duplicate_row", "medium", "No duplicate rows",
          lambda: {"success": int(dup_rows.sum()) == 0, "details": {"duplicate_count": int(dup_rows.sum())}})

    if "CustomerId" in df.columns:
        dup_id = df["CustomerId"].duplicated(keep=False)
        check("duplicate_id", "medium", "CustomerId must be unique",
              lambda: {"success": int(dup_id.sum()) == 0, "details": {"duplicate_id_count": int(dup_id.sum())}})

    format_mask = df.apply(
        lambda col: col.astype(str).str.contains(r'^(ERR_|INVALID)', regex=True, na=False)
    ).any(axis=1)

    check("format_error", "medium", "No ERR_ prefixed or INVALID values",
          lambda: {"success": not bool(format_mask.any()), "details": {"format_error_rows": int(format_mask.sum())}})

    # Calculate bad rows
    bad_mask = pd.Series(False, index=df.index)
    bad_mask |= df[required].isnull().any(axis=1)

    age_num = pd.to_numeric(df["Age"], errors="coerce")
    bad_mask |= age_num.isna() | (age_num < 0) | (age_num > 120)

    inc_num = pd.to_numeric(df["EstimatedSalary"], errors="coerce")
    bad_mask |= inc_num.isna() | (inc_num < 0) | (inc_num > 1_000_000)

    bad_mask |= ~df["Gender"].isin(["Male", "Female"])
    bad_mask |= ~df["Geography"].isin(["France", "Spain", "Germany"])
    bad_mask |= df.duplicated(keep=False)

    if "CustomerId" in df.columns:
        bad_mask |= df["CustomerId"].duplicated(keep=False)

    bad_mask |= format_mask

    numeric_cols = ["HasCrCard", "IsActiveMember", "NumOfProducts", "CreditScore", "Tenure", "Balance"]
    for col in numeric_cols:
        if col in df.columns:
            col_num = pd.to_numeric(df[col], errors="coerce")
            bad_mask |= col_num.isna()

    bad_idx = df.index[bad_mask].tolist()
    good_idx = df.index[~bad_mask].tolist()

    # Generate Great Expectations Data Docs HTML report
    report_url = None
    try:

        suite = ge_df.get_expectation_suite()


        docs_dir = REPORTS_DIR / "data_docs_ge"
        docs_dir.mkdir(parents=True, exist_ok=True)

        from great_expectations.render.renderer import ExpectationSuitePageRenderer
        from great_expectations.render.view import DefaultJinjaPageView


        renderer = ExpectationSuitePageRenderer()
        document = renderer.render(suite)


        view = DefaultJinjaPageView()
        html = view.render(document)

        report_filename = f"{uuid.uuid4().hex}.html"
        report_path = REPORTS_DIR / report_filename
        report_path.write_text(html, encoding="utf-8")

        report_url = f"{FASTAPI_REPORT_URL}/{report_filename}"
        print(f" Great Expectations Data Docs report created: {report_filename}")

    except Exception as e:
        print(f" Could not create GE Data Docs report: {e}")
        import traceback
        traceback.print_exc()

    print(f" Valid: {len(good_idx)} |  Invalid: {len(bad_idx)}")
    print(f" Criticality: {criticality.upper()}")
    print(f" Errors found: {len(errors)}")
    print(f"{'=' * 60}\n")

    return {
        "file_path": file_path,
        "errors": errors,
        "criticality": criticality,
        "valid_rows": len(good_idx),
        "invalid_rows": len(bad_idx),
        "expectations": results,
        "bad_row_indices": bad_idx,
        "good_row_indices": good_idx,
        "ge_report_url": report_url
    }


@task(dag=dag)
def send_alerts(validation: Dict) -> str:
    """Send Teams alert"""

    print(f"\n{'=' * 60}")
    print("TASK 3: Sending alerts")
    print(f"{'=' * 60}\n")

    report_url = validation.get("ge_report_url")

    if not report_url:
        print(" No report URL found")
        report_url = f"{FASTAPI_REPORT_URL}/no-report.html"

    print(f" Report URL: {report_url}\n")

    if validation["invalid_rows"] > 0:
        send_teams_alert(validation, report_url)
        print(" Teams alert sent")
    else:
        print(" No errors - skipping alert")

    return report_url


@task(dag=dag)
def save_statistics(validation: Dict):

    print(f"\n{'=' * 60}")
    print("TASK 4: Saving to database")
    print(f"{'=' * 60}\n")

    Base.metadata.create_all(bind=engine)
    db = SessionLocal()

    try:
        entry = IngestionStatistic(
            file_name=Path(validation["file_path"]).name,
            total_rows=validation["valid_rows"] + validation["invalid_rows"],
            valid_rows=validation["valid_rows"],
            invalid_rows=validation["invalid_rows"],
            criticality=validation["criticality"],
            report_path=validation.get("ge_report_url"),
        )
        db.add(entry)
        db.commit()
        print(f" Saved to DB\n")
    except Exception as e:
        db.rollback()
        print(f" DB error: {e}\n")
        raise
    finally:
        db.close()


@task(dag=dag)
def split_and_save(validation: Dict):


    print(f"\n{'=' * 60}")
    print("TASK 5: Splitting and archiving")
    print(f"{'=' * 60}\n")

    GOOD_DIR.mkdir(parents=True, exist_ok=True)
    BAD_DIR.mkdir(parents=True, exist_ok=True)
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

    raw_path = Path(validation["file_path"])

    if not raw_path.exists():
        print(" File already processed\n")
        return

    df = pd.read_csv(raw_path)
    bad_idx = validation.get("bad_row_indices", [])
    good_idx = validation.get("good_row_indices", [])

    if validation["invalid_rows"] == 0:
        df.to_csv(GOOD_DIR / raw_path.name, index=False)
        raw_path.rename(ARCHIVE_DIR / raw_path.name)
        print(f" All valid → good_data/\n")

    elif validation["valid_rows"] == 0:
        df.to_csv(BAD_DIR / raw_path.name, index=False)
        raw_path.rename(ARCHIVE_DIR / raw_path.name)
        print(f" All invalid → bad_data/\n")

    else:
        good_df = df.loc[good_idx]
        bad_df = df.loc[bad_idx]

        good_df.to_csv(GOOD_DIR / f"{raw_path.stem}_good.csv", index=False)
        bad_df.to_csv(BAD_DIR / f"{raw_path.stem}_bad.csv", index=False)
        raw_path.rename(ARCHIVE_DIR / raw_path.name)

        print(f" Good: {len(good_df)} rows")
        print(f" Bad: {len(bad_df)} rows\n")


raw_file = read_data()
validation = validate_data(raw_file)
alert_task = send_alerts(validation)
split_task = split_and_save(validation)
save_task = save_statistics(validation)

validation >> [alert_task, split_task, save_task]