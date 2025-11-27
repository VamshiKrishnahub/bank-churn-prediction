# airflow/dags/data_ingestion_dag.py

from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowSkipException

import pandas as pd
import os
import random
import uuid

from pathlib import Path
from typing import Dict, List
import great_expectations as ge

from database.db import (
    Base,
    SessionLocal,
    engine,
    IngestionStatistic,
)

from send_alerts import send_teams_alert   # 🔥 TEAMS ALERT FUNCTION


# ===============================================================
# CONFIG
# ===============================================================
DATA_DIR = Path("/opt/airflow/Data")
RAW_DATA_SOURCE = DATA_DIR / "raw-data"
LEGACY_RAW_DIR = DATA_DIR / "raw"
GOOD_DIR = DATA_DIR / "good_data"
BAD_DIR = DATA_DIR / "bad_data"
REPORTS_DIR = Path("/opt/airflow/Data/reports")


FASTAPI_REPORT_URL = "http://fastapi:8000/reports"  # 🔥 IMPORTANT

default_args = {
    "owner": "team",
    "depends_on_past": False,
    "retries": 0,
}


# ===============================================================
# DAG DEFINITION
# ===============================================================
with DAG(
    dag_id="data_ingestion_dag",
    default_args=default_args,
    schedule="*/1 * * * *",   # every minute
    start_date=days_ago(1),
    catchup=False,
    tags=["ingestion"],
):

    # -----------------------------------------------------------
    # 1️⃣ READ ONE CSV FROM raw-data
    # -----------------------------------------------------------
    @task
    def read_data() -> str:
        RAW_DATA_SOURCE.mkdir(exist_ok=True)

        available = list(RAW_DATA_SOURCE.glob("*.csv"))
        if not available:
            available = list(LEGACY_RAW_DIR.glob("*.csv"))

        if not available:
            raise AirflowSkipException("No input CSV files found")

        chosen = random.choice(available)
        print(f"Selected file → {chosen}")
        return str(chosen)

    # -----------------------------------------------------------
    # 2️⃣ VALIDATE USING GREAT EXPECTATIONS
    # -----------------------------------------------------------
    @task
    def validate_data(file_path: str):

        df = pd.read_csv(file_path)
        ge_df = ge.from_pandas(df)

        errors: List[Dict] = []
        results: List[Dict] = []
        severity_rank = {"low": 1, "medium": 2, "high": 3}
        criticality = "low"

        def serialize(obj):
            if hasattr(obj, "to_json_dict"):
                return obj.to_json_dict()
            if hasattr(obj, "to_dict"):
                return obj.to_dict()
            if isinstance(obj, dict):
                return obj
            return {"raw": str(obj)}

        def check(name, severity, description, fn):
            nonlocal criticality

            try:
                out = fn()
            except Exception as exc:
                out = {"success": False, "error": str(exc)}

            out = serialize(out)

            results.append(
                {
                    "check": name,
                    "description": description,
                    "severity": severity,
                    "result": out,
                }
            )

            if not out.get("success", False):
                errors.append(
                    {
                        "type": name,
                        "message": description,
                        "criticality": severity,
                        "details": out,
                    }
                )
                if severity_rank[severity] > severity_rank[criticality]:
                    criticality = severity

        # Required fields
        required = ["age", "gender", "country", "income"]

        # Missing column detection
        for col in required:
            check(
                "missing_column",
                "high",
                f"Column '{col}' must exist",
                lambda col=col: ge_df.expect_column_to_exist(col),
            )

        # Continue validations if ALL columns exist
        if all(c in df.columns for c in required):

            # Missing values
            for col in required:
                check(
                    "missing_values",
                    "medium",
                    f"Column '{col}' cannot be null",
                    lambda col=col: ge_df.expect_column_values_to_not_be_null(col),
                )

            # Age numeric
            check(
                "age_numeric",
                "high",
                "Age must be numeric",
                lambda: ge_df.expect_column_values_to_be_in_type_list(
                    "age", ["int64", "float64"]
                ),
            )

            # Age range
            check(
                "age_range",
                "high",
                "Age must be 0–120",
                lambda: ge_df.expect_column_values_to_be_between("age", 0, 120),
            )

            # Gender valid
            check(
                "gender_valid",
                "high",
                "Gender must be male/female",
                lambda: ge_df.expect_column_values_to_be_in_set(
                    "gender", ["male", "female"]
                ),
            )

            # Country valid
            check(
                "country_valid",
                "medium",
                "Country must be China, India, Lebanon",
                lambda: ge_df.expect_column_values_to_be_in_set(
                    "country", ["China", "India", "Lebanon"]
                ),
            )

            # Income range
            check(
                "income_range",
                "high",
                "Income must be 0–1,000,000",
                lambda: ge_df.expect_column_values_to_be_between(
                    "income", 0, 1_000_000
                ),
            )

            # Duplicate rows
            check(
                "duplicates",
                "medium",
                "No duplicate rows",
                lambda: {
                    "success": not df.duplicated().any(),
                    "details": {"duplicate_count": int(df.duplicated().sum())},
                },
            )

        invalid_rows = int(df.isnull().any(axis=1).sum())
        valid_rows = len(df) - invalid_rows

        return {
            "file_path": file_path,
            "errors": errors,
            "criticality": criticality,
            "valid_rows": valid_rows,
            "invalid_rows": invalid_rows,
            "expectations": results,
        }

    # -----------------------------------------------------------
    # 3️⃣ SAVE STATS TO DB
    # -----------------------------------------------------------
    @task
    def save_statistics(validation):
        Base.metadata.create_all(bind=engine)
        db = SessionLocal()

        try:
            new = IngestionStatistic(
                file_name=Path(validation["file_path"]).name,
                total_rows=validation["valid_rows"] + validation["invalid_rows"],
                valid_rows=validation["valid_rows"],
                invalid_rows=validation["invalid_rows"],
                criticality=validation["criticality"],
                report_path=None,
            )
            db.add(new)
            db.commit()

        finally:
            db.close()

    # -----------------------------------------------------------
    # 4️⃣ GENERATE HTML REPORT + SEND TEAMS ALERT
    # -----------------------------------------------------------
    @task
    def send_alerts(validation):
        REPORTS_DIR.mkdir(exist_ok=True)

        report_file = f"{uuid.uuid4().hex}.html"
        report_path = REPORTS_DIR / report_file

        # Error list
        error_list = "".join(
            f"<li><b>{e['type']}</b> ({e['criticality']}): {e['message']}</li>"
            for e in validation["errors"]
        )

        # Expectation table
        expectation_rows = "".join(
            f"<tr><td>{r['check']}</td><td>{r['severity']}</td>"
            f"<td>{r['description']}</td><td>{r['result'].get('success')}</td></tr>"
            for r in validation["expectations"]
        )

        html = f"""
        <h1>Data Validation Report</h1>
        <p><b>File:</b> {validation['file_path']}</p>
        <p><b>Criticality:</b> {validation['criticality']}</p>

        <h3>Errors ({len(validation['errors'])})</h3>
        <ul>{error_list}</ul>

        <h3>Expectation Results</h3>
        <table border="1" cellpadding="4">
            <tr>
                <th>Check</th><th>Severity</th><th>Description</th><th>Success</th>
            </tr>
            {expectation_rows}
        </table>
        """

        report_path.write_text(html)
        print(f"HTML report created → {report_path}")

        # 🔥 CLICKABLE PUBLIC LINK
        public_link = f"{FASTAPI_REPORT_URL}/{report_file}"

        # 🔥 Send Teams alert
        send_teams_alert(validation, public_link)

        return public_link

    # -----------------------------------------------------------
    # 5️⃣ SPLIT INTO GOOD/BAD + ARCHIVE FILE
    # -----------------------------------------------------------
    @task
    def split_and_save(validation):

        GOOD_DIR.mkdir(exist_ok=True)
        BAD_DIR.mkdir(exist_ok=True)
        ARCHIVE_DIR = DATA_DIR / "archive_raw"
        ARCHIVE_DIR.mkdir(exist_ok=True)

        raw = Path(validation["file_path"])
        df = pd.read_csv(raw)

        good = df.dropna()
        bad = df[df.isnull().any(axis=1)]

        if bad.empty:
            good.to_csv(GOOD_DIR / raw.name, index=False)
        elif good.empty:
            bad.to_csv(BAD_DIR / raw.name, index=False)
        else:
            good.to_csv(GOOD_DIR / raw.name, index=False)
            bad.to_csv(BAD_DIR / ("BAD_" + raw.name), index=False)

        archived = ARCHIVE_DIR / raw.name
        raw.rename(archived)

    # -----------------------------------------------------------
    # DAG PIPELINE
    # -----------------------------------------------------------
    raw_file = read_data()
    validation = validate_data(raw_file)

    save_task = save_statistics(validation)
    alert_task = send_alerts(validation)
    split_task = split_and_save(validation)

    validation >> [save_task, alert_task, split_task]
