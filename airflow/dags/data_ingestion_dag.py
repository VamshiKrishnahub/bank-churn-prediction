from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowSkipException

import os
import random
import uuid
from pathlib import Path
from typing import Dict, List

import pandas as pd
import numpy as np
import great_expectations as ge

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

FASTAPI_REPORT_URL = "http://fastapi:8000/reports"

default_args = {
    "owner": "team",
    "depends_on_past": False,
    "retries": 0,
    "max_active_runs": 1,  # ← FIX: Only 1 DAG run at a time
}

dag = DAG(
    dag_id="data_ingestion_dag",
    default_args=default_args,
    description="Data ingestion pipeline with Great Expectations validation",
    schedule="*/1 * * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,  # ← FIX: Prevent concurrent runs
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

    # Random selection
    chosen = random.choice(available)

    print(f"📌 Selected: {chosen.name}")
    print(f"   Total files available: {len(available)}")
    print(f"{'=' * 60}\n")

    return str(chosen)


@task(dag=dag)
def validate_data(file_path: str) -> Dict:
    """
    Validate data using Great Expectations.
    """

    print(f"\n{'=' * 60}")
    print("TASK 2: Validating data")
    print(f"{'=' * 60}\n")

    try:
        df = pd.read_csv(file_path)
        print(f"✅ Loaded: {len(df)} rows")
    except Exception as exc:
        raise AirflowSkipException(f"❌ Cannot read file: {exc}")

    if df.empty:
        raise AirflowSkipException("⚠️ Empty file")

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
        results.append({"check": name, "description": description, "severity": severity, "result": out})

        if not out.get("success", False):
            errors.append({"type": name, "message": description, "criticality": severity, "details": out})
            if severity_rank[severity] > severity_rank[criticality]:
                criticality = severity

    # Required columns
    required = ["Age", "Gender", "Geography", "EstimatedSalary"]

    for col in required:
        check("missing_column", "high", f"Column '{col}' must exist",
              lambda col=col: ge_df.expect_column_to_exist(col))

    if not all(c in df.columns for c in required):
        return {
            "file_path": file_path, "errors": errors, "criticality": "high",
            "valid_rows": 0, "invalid_rows": len(df), "expectations": results,
            "bad_row_indices": list(range(len(df))), "good_row_indices": []
        }

    # Missing values
    for col in required:
        check("missing_value", "medium", f"Column '{col}' cannot have nulls",
              lambda col=col: ge_df.expect_column_values_to_not_be_null(col))

    # Type check
    def check_age_numeric():
        age_num = pd.to_numeric(df["Age"], errors="coerce")
        non_num = age_num.isna() & df["Age"].notna()
        return {"success": int(non_num.sum()) == 0, "details": {"non_numeric_age_count": int(non_num.sum())}}

    check("value_error", "high", "Age must be numeric", check_age_numeric)

    # Range checks
    check("out_of_range_age", "high", "Age must be 0-120",
          lambda: ge_df.expect_column_values_to_be_between("Age", 0, 120))

    check("out_of_range_income", "high", "EstimatedSalary must be 0-1,000,000",
          lambda: ge_df.expect_column_values_to_be_between("EstimatedSalary", 0, 1_000_000))

    # Categorical
    check("categorical_error_gender", "high", "Gender must be Male/Female",
          lambda: ge_df.expect_column_values_to_be_in_set("Gender", ["Male", "Female"]))

    check("categorical_error_geography", "medium", "Geography must be France/Spain/Germany",
          lambda: ge_df.expect_column_values_to_be_in_set("Geography", ["France", "Spain", "Germany"]))

    # Duplicates
    dup_rows = df.duplicated(keep=False)
    check("duplicate_row", "medium", "No duplicate rows",
          lambda: {"success": int(dup_rows.sum()) == 0, "details": {"duplicate_count": int(dup_rows.sum())}})

    if "CustomerId" in df.columns:
        dup_id = df["CustomerId"].duplicated(keep=False)
        check("duplicate_id", "medium", "CustomerId must be unique",
              lambda: {"success": int(dup_id.sum()) == 0, "details": {"duplicate_id_count": int(dup_id.sum())}})

    # Format errors
    format_mask = df.apply(lambda col: col.astype(str).str.startswith("ERR_")).any(axis=1)
    check("format_error", "medium", "No ERR_ prefixed values",
          lambda: {"success": not bool(format_mask.any()), "details": {"format_error_rows": int(format_mask.sum())}})

    # Build bad row mask
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

    bad_idx = df.index[bad_mask].tolist()
    good_idx = df.index[~bad_mask].tolist()

    print(f"✅ Valid: {len(good_idx)} | ❌ Invalid: {len(bad_idx)}")
    print(f"🚨 Criticality: {criticality.upper()}\n")

    return {
        "file_path": file_path, "errors": errors, "criticality": criticality,
        "valid_rows": len(good_idx), "invalid_rows": len(bad_idx),
        "expectations": results, "bad_row_indices": bad_idx, "good_row_indices": good_idx
    }


@task(dag=dag)
def send_alerts(validation: Dict) -> str:
    """
    Generate simple HTML report with errors only.
    """

    print(f"\n{'=' * 60}")
    print("TASK 3: Generating report")
    print(f"{'=' * 60}\n")

    REPORTS_DIR.mkdir(exist_ok=True)
    report_file = f"{uuid.uuid4().hex}.html"
    report_path = REPORTS_DIR / report_file

    # Build error table
    error_rows = ""
    for e in validation["errors"]:
        error_type = e['type']
        details = e.get('details', {})

        # Determine what was found
        if error_type == "missing_column":
            found = "Column missing"
        elif error_type == "missing_value":
            found = "Null values found"
        elif error_type == "value_error":
            count = details.get('non_numeric_age_count', 0)
            found = f"{count} non-numeric value(s)"
        elif error_type == "out_of_range_age":
            found = "Values outside 0-120"
        elif error_type == "out_of_range_income":
            found = "Values outside 0-1M"
        elif error_type.startswith("categorical_error"):
            found = "Invalid values"
        elif error_type == "duplicate_row":
            count = details.get('duplicate_count', 0)
            found = f"{count} duplicate(s)"
        elif error_type == "duplicate_id":
            count = details.get('duplicate_id_count', 0)
            found = f"{count} duplicate ID(s)"
        elif error_type == "format_error":
            count = details.get('format_error_rows', 0)
            found = f"{count} row(s) with ERR_"
        else:
            found = "Error detected"

        error_rows += f"""
        <tr>
            <td>{error_type}</td>
            <td class="{e['criticality']}">{e['criticality'].upper()}</td>
            <td>{e['message']}</td>
            <td>{found}</td>
        </tr>
        """

    if not validation["errors"]:
        error_rows = "<tr><td colspan='4' style='text-align:center; color:green;'>✅ No errors</td></tr>"

    total = validation['valid_rows'] + validation['invalid_rows']
    valid_pct = (validation['valid_rows'] / total * 100) if total > 0 else 0
    invalid_pct = (validation['invalid_rows'] / total * 100) if total > 0 else 0

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Data Validation Report</title>
        <style>
            body {{ font-family: Arial; margin: 40px; }}
            h1 {{ color: #333; border-bottom: 2px solid #333; padding-bottom: 10px; }}
            table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
            th, td {{ border: 1px solid #ddd; padding: 10px; text-align: left; }}
            th {{ background-color: #f2f2f2; }}
            .high {{ color: red; font-weight: bold; }}
            .medium {{ color: orange; font-weight: bold; }}
            .low {{ color: blue; font-weight: bold; }}
        </style>
    </head>
    <body>
        <h1>Data Validation Report</h1>

        <h3>Summary</h3>
        <p><b>File:</b> {Path(validation['file_path']).name}</p>
        <p><b>Total Rows:</b> {total}</p>
        <p><b>Valid Rows:</b> {validation['valid_rows']} ({valid_pct:.1f}%)</p>
        <p><b>Invalid Rows:</b> {validation['invalid_rows']} ({invalid_pct:.1f}%)</p>
        <p><b>Criticality:</b> <span class="{validation['criticality']}">{validation['criticality'].upper()}</span></p>

        <h3>Errors ({len(validation['errors'])})</h3>
        <table>
            <tr>
                <th>Error Type</th>
                <th>Severity</th>
                <th>Expected</th>
                <th>What Was Found</th>
            </tr>
            {error_rows}
        </table>
    </body>
    </html>
    """

    report_path.write_text(html, encoding="utf-8")
    public_url = f"{FASTAPI_REPORT_URL}/{report_file}"

    print(f"✅ Report: {public_url}\n")

    if validation["invalid_rows"] > 0:
        send_teams_alert(validation, public_url)

    return public_url

@task(dag=dag)
def save_statistics(validation: Dict, report_link: str = None):
    """
    Save stats to database.
    """
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
            report_path=report_link,
        )
        db.add(entry)
        db.commit()
        print(f"✅ Saved to DB\n")
    except Exception as e:
        db.rollback()
        print(f"❌ DB error: {e}\n")
        raise
    finally:
        db.close()


@task(dag=dag)
def split_and_save(validation: Dict):
    """
    Split good/bad data and archive.
    """

    print(f"\n{'=' * 60}")
    print("TASK 5: Splitting and archiving")
    print(f"{'=' * 60}\n")

    GOOD_DIR.mkdir(exist_ok=True)
    BAD_DIR.mkdir(exist_ok=True)
    ARCHIVE_DIR.mkdir(exist_ok=True)

    raw_path = Path(validation["file_path"])

    if not raw_path.exists():
        print("⚠️ File already processed\n")
        return

    df = pd.read_csv(raw_path)
    bad_idx = validation.get("bad_row_indices", [])
    good_idx = validation.get("good_row_indices", [])

    if validation["invalid_rows"] == 0:
        # All valid
        df.to_csv(GOOD_DIR / raw_path.name, index=False)
        raw_path.rename(ARCHIVE_DIR / raw_path.name)
        print(f"✅ All valid → good_data/\n")

    elif validation["valid_rows"] == 0:
        # All invalid
        df.to_csv(BAD_DIR / raw_path.name, index=False)
        raw_path.rename(ARCHIVE_DIR / raw_path.name)
        print(f"❌ All invalid → bad_data/\n")

    else:
        # Split
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
save_task = save_statistics(validation, alert_task)

validation >> [alert_task, split_task, save_task]