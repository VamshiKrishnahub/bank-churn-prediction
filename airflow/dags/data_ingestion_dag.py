# airflow/dags/data_ingestion_dag.py
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowSkipException

import pandas as pd
import os
import random
import requests
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

# -------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------
DATA_DIR = Path("/opt/airflow/Data")
RAW_DATA_SOURCE = DATA_DIR / "raw-data"
LEGACY_RAW_DIR = DATA_DIR / "raw"
GOOD_DIR = DATA_DIR / "good_data"
BAD_DIR = DATA_DIR / "bad_data"
REPORTS_DIR = DATA_DIR / "reports"

TEAMS_WEBHOOK = os.environ.get("TEAMS_WEBHOOK")

default_args = {
    "owner": "team",
    "depends_on_past": False,
    "retries": 0,
}

# -------------------------------------------------------------
# DAG
# -------------------------------------------------------------
with DAG(
    dag_id="data_ingestion_dag",
    default_args=default_args,
    schedule="*/1 * * * *",  # every minute
    start_date=days_ago(1),
    catchup=False,
    tags=["ingestion"],
):

    # 1️⃣ READ ONE RAW FILE
    @task
    def read_data() -> str:
        RAW_DATA_SOURCE.mkdir(exist_ok=True)

        available = list(RAW_DATA_SOURCE.glob("*.csv"))
        if not available:
            available = list(LEGACY_RAW_DIR.glob("*.csv"))

        if not available:
            raise AirflowSkipException(
                f"No CSV files found in {RAW_DATA_SOURCE} or {LEGACY_RAW_DIR}"
            )

        file_path = random.choice(available)
        print(f"Selected RAW file: {file_path}")
        return str(file_path)

    # 2️⃣ VALIDATE WITH GREAT EXPECTATIONS
    @task
    def validate_data(file_path: str):
        df = pd.read_csv(file_path)
        ge_df = ge.from_pandas(df)

        errors: List[Dict] = []
        expectation_results: List[Dict] = []
        severity_rank = {"low": 1, "medium": 2, "high": 3}
        criticality = "low"

        def serialize(result):
            if hasattr(result, "to_json_dict"):
                return result.to_json_dict()
            if hasattr(result, "to_dict"):
                return result.to_dict()
            if isinstance(result, dict):
                return result
            return {"raw": str(result)}

        def check(name, severity, description, func):
            nonlocal criticality
            try:
                res = func()
            except Exception as exc:
                res = {"success": False, "error": str(exc)}

            sres = serialize(res)
            expectation_results.append(
                {
                    "check": name,
                    "description": description,
                    "severity": severity,
                    "result": sres,
                }
            )

            if not sres.get("success", False):
                errors.append(
                    {
                        "type": name,
                        "message": description,
                        "criticality": severity,
                        "details": sres,
                    }
                )
                if severity_rank[severity] > severity_rank[criticality]:
                    criticality = severity

        required = ["age", "gender", "country", "income"]
        for col in required:
            check(
                "missing_column",
                "high",
                f"Column '{col}' must exist.",
                lambda col=col: ge_df.expect_column_to_exist(col),
            )

        if all(col in df.columns for col in required):
            for col in required:
                check(
                    "missing_values",
                    "medium",
                    f"Column '{col}' cannot contain nulls.",
                    lambda col=col: ge_df.expect_column_values_to_not_be_null(col),
                )

            check(
                "age_numeric",
                "high",
                "Age must be numeric.",
                lambda: ge_df.expect_column_values_to_be_in_type_list(
                    "age", ["int64", "float64", "float32", "int32"]
                ),
            )

            check(
                "age_range",
                "high",
                "Age must be between 0 and 120.",
                lambda: ge_df.expect_column_values_to_be_between("age", 0, 120),
            )

            check(
                "gender_valid",
                "high",
                "Gender must be 'male' or 'female'.",
                lambda: ge_df.expect_column_values_to_be_in_set(
                    "gender", ["male", "female"]
                ),
            )

            check(
                "country_valid",
                "medium",
                "Country must be China, India, or Lebanon.",
                lambda: ge_df.expect_column_values_to_be_in_set(
                    "country", ["China", "India", "Lebanon"]
                ),
            )

            check(
                "income_range",
                "high",
                "Income must be positive and < 1,000,000.",
                lambda: ge_df.expect_column_values_to_be_between(
                    "income", 0, 1_000_000
                ),
            )

            check(
                "duplicates",
                "medium",
                "Dataset should not contain duplicate rows.",
                lambda: {
                    "success": not df.duplicated().any(),
                    "details": {"duplicate_count": int(df.duplicated().sum())},
                },
            )

        invalid_rows = int(df.isnull().any(axis=1).sum())
        valid_rows = len(df) - invalid_rows

        result = {
            "file_path": file_path,
            "errors": errors,
            "criticality": criticality,
            "valid_rows": valid_rows,
            "invalid_rows": invalid_rows,
            "expectations": expectation_results,
        }

        print("VALIDATION RESULT:", result)
        return result

    # 3️⃣ SAVE STATISTICS TO DB
    @task
    def save_statistics(validation):
        Base.metadata.create_all(bind=engine)
        session = SessionLocal()

        try:
            stat = IngestionStatistic(
                file_name=Path(validation["file_path"]).name,
                total_rows=validation["valid_rows"] + validation["invalid_rows"],
                valid_rows=validation["valid_rows"],
                invalid_rows=validation["invalid_rows"],
                criticality=validation["criticality"],
                report_path=None,
            )
            session.add(stat)
            session.commit()
            print("Saved stats:", stat.file_name)
        finally:
            session.close()

    # 4️⃣ SEND ALERTS
    @task
    def send_alerts(validation):
        REPORTS_DIR.mkdir(exist_ok=True)
        report_path = REPORTS_DIR / f"{uuid.uuid4().hex}_report.html"

        error_list_items = "".join(
            f"<li><b>{e['type']}</b> ({e['criticality']}): {e['message']}</li>"
            for e in validation["errors"]
        )

        expectation_rows = "".join(
            f"<tr><td>{res['check']}</td><td>{res['severity']}</td>"
            f"<td>{res['description']}</td><td>{res['result'].get('success')}</td></tr>"
            for res in validation["expectations"]
        )

        html = f"""
        <h1>Data Validation Report</h1>
        <p><b>File:</b> {validation['file_path']}</p>
        <p><b>Criticality:</b> {validation['criticality']}</p>
        <h3>Errors ({len(validation['errors'])})</h3>
        <ul>{error_list_items}</ul>
        <h3>Expectation Results</h3>
        <table border="1">
            <tr><th>Check</th><th>Severity</th><th>Description</th><th>Success</th></tr>
            {expectation_rows}
        </table>
        """
        report_path.write_text(html)
        print(f"Report created: {report_path}")
        return str(report_path)

    # 5️⃣ SPLIT + ARCHIVE (UPDATED)
    @task
    def split_and_save(validation):
        GOOD_DIR.mkdir(exist_ok=True)
        BAD_DIR.mkdir(exist_ok=True)

        ARCHIVE_DIR = DATA_DIR / "archive_raw"
        ARCHIVE_DIR.mkdir(exist_ok=True)

        raw_path = Path(validation["file_path"])
        df = pd.read_csv(raw_path)
        file_name = raw_path.name

        good_df = df.dropna()
        bad_df = df[df.isnull().any(axis=1)]

        if bad_df.empty:
            good_df.to_csv(GOOD_DIR / file_name, index=False)
        elif good_df.empty:
            bad_df.to_csv(BAD_DIR / file_name, index=False)
        else:
            good_df.to_csv(GOOD_DIR / file_name, index=False)
            bad_df.to_csv(BAD_DIR / f"BAD_{file_name}", index=False)

        print(f"Saved good/bad splits for {file_name}")

        # ARCHIVE RAW FILE
        archived_path = ARCHIVE_DIR / file_name
        raw_path.rename(archived_path)
        print(f"Archived raw file → {archived_path}")

    # DAG FLOW
    raw_file = read_data()
    validation = validate_data(raw_file)

    save_task = save_statistics(validation)
    alert_task = send_alerts(validation)
    split_task = split_and_save(validation)

    validation >> [save_task, alert_task, split_task]
