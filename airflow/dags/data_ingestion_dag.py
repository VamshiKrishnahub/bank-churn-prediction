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
from great_expectations.checkpoint import SimpleCheckpoint

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


def clean_for_json(obj):
    if isinstance(obj, dict):
        return {k: clean_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_for_json(item) for item in obj]
    elif isinstance(obj, float):
        if np.isnan(obj) or np.isinf(obj):
            return None
        return obj
    else:
        return obj


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

    print(f"📄 Selected: {chosen.name}")
    print(f"   Total files available: {len(available)}")
    print(f"{'=' * 60}\n")

    return str(chosen)


@task(dag=dag)
def validate_data(file_path: str) -> Dict:

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

    # Load GE context using get_context() - more robust than FileDataContext
    print("📁 Loading GE context...")

    # Your structure: /opt/airflow/Data/great_expectations/gx/great_expectations.yml
    ge_gx_dir = GE_DIR / "gx"
    ge_config_path = ge_gx_dir / "great_expectations.yml"

    if ge_config_path.exists():
        print(f"   Found config at: {ge_config_path}")

        # Use get_context() which is smarter about finding configs
        try:
            from great_expectations.data_context import get_context
            context = get_context(context_root_dir=str(ge_gx_dir))
            print(" GE context loaded successfully")
        except Exception as e:
            print(f" Failed to load with get_context: {e}")


            try:
                context = FileDataContext(GE_DIR)
                print(" GE context loaded from parent directory")
            except Exception as e2:
                print(f" All methods failed:")
                print(f"   get_context: {e}")
                print(f"   FileDataContext: {e2}")
                raise Exception(f"Could not load GE context. Please check your GE setup at {GE_DIR}")
    else:
        raise Exception(f"GE config not found at {ge_config_path}. Please check your GE setup.")

    # Configure Data Docs to save in reports/data_docs directory (separate from data_docs_ge)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    data_docs_path = REPORTS_DIR / "data_docs"

    print(f" Configuring Data Docs to: {data_docs_path}")
    try:
        context_config = context.get_config()
        context_config.data_docs_sites = {
            "local_site": {
                "class_name": "SiteBuilder",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": str(data_docs_path)
                },
                "site_index_builder": {
                    "class_name": "DefaultSiteIndexBuilder"
                }
            }
        }
        context._project_config = context_config
        print(" Data Docs configured")
    except Exception as e:
        print(f" Data Docs config warning: {e}")

    # Get datasource and create batch request
    datasource_name = "pandas_datasource"
    data_connector_name = "runtime_data_connector"

    run_id = f"run_{uuid.uuid4().hex[:8]}"
    file_name = Path(file_path).name

    batch_request = RuntimeBatchRequest(
        datasource_name=datasource_name,
        data_connector_name=data_connector_name,
        data_asset_name="churn_data",
        runtime_parameters={"batch_data": df},
        batch_identifiers={
            "batch_id": Path(file_path).stem,
            "file_name": file_name  # Use file_name instead of run_id to match datasource config
        }
    )

    # Get expectation suite
    suite_name = "churn_data_validation_suite"
    suite = context.get_expectation_suite(suite_name)
    print(f" Using suite: {suite_name}")

    # Create checkpoint with UpdateDataDocsAction to generate HTML validation report
    checkpoint_name = f"validation_checkpoint_{run_id}"
    checkpoint = SimpleCheckpoint(
        name=checkpoint_name,
        data_context=context,
        action_list=[
            {
                "name": "store_validation_result",
                "action": {"class_name": "StoreValidationResultAction"}
            },
            {
                "name": "update_data_docs",
                "action": {
                    "class_name": "UpdateDataDocsAction",
                    "site_names": ["local_site"]
                }
            }
        ]
    )

    print(" Running validation with checkpoint...")
    checkpoint_result = checkpoint.run(
        validations=[
            {
                "batch_request": batch_request,
                "expectation_suite_name": suite_name
            }
        ]
    )

    validation_result = list(checkpoint_result.run_results.values())[0]["validation_result"]
    print(" Validation completed")

    # Build Data Docs to generate HTML
    print(" Building Data Docs HTML...")
    context.build_data_docs(site_names=["local_site"])
    print(" Data Docs built")

    # Get validation report URL
    report_url = None

    try:
        # Method 1: Get URL from checkpoint result identifiers
        ids = checkpoint_result.list_validation_result_identifiers()
        print(f" Found {len(ids)} validation result identifier(s)")

        if ids:
            docs_urls = context.get_docs_sites_urls(resource_identifier=ids[0])
            print(f" Docs URLs: {docs_urls}")

            if docs_urls and docs_urls[0].get("site_url"):
                site_url = docs_urls[0]["site_url"]
                print(f" Site URL: {site_url}")

                if site_url.startswith("file://"):
                    file_path_str = site_url.replace("file://", "")
                    file_path_obj = Path(file_path_str)

                    try:
                        # Get path relative to REPORTS_DIR
                        relative_path = file_path_obj.relative_to(REPORTS_DIR)
                        report_url = f"{FASTAPI_REPORT_URL}/{relative_path.as_posix()}"
                        print(f" Report URL generated: {report_url}")
                    except ValueError:
                        print(f" File not in reports directory: {file_path_str}")

    except Exception as e:
        print(f" Error getting URL from identifiers: {e}")
        import traceback
        traceback.print_exc()

    # Method 2: Fallback - manually find the HTML file
    if not report_url:
        print(" Attempting manual HTML search...")

        validations_path = data_docs_path / "local_site" / "validations" / suite_name

        if validations_path.exists():
            html_files = list(validations_path.rglob("*.html"))
            print(f" Found {len(html_files)} HTML file(s)")

            if html_files:
                # Get most recent HTML file
                latest_html = max(html_files, key=lambda p: p.stat().st_mtime)
                relative_path = latest_html.relative_to(REPORTS_DIR)
                report_url = f"{FASTAPI_REPORT_URL}/{relative_path.as_posix()}"
                print(f" Found HTML manually: {report_url}")
        else:
            print(f" Validations path doesn't exist: {validations_path}")

    if report_url:
        print(f" Final GE Data Docs validation report URL: {report_url}")
    else:
        print(" Could not generate Data Docs report URL")

    errors: List[Dict] = []
    results: List[Dict] = []
    severity_rank = {"low": 1, "medium": 2, "high": 3}
    criticality = "low"

    def serialize(obj):
        if hasattr(obj, "to_json_dict"):
            result = obj.to_json_dict()
            return clean_for_json(result)
        if isinstance(obj, dict):
            return clean_for_json(obj)
        return {"raw": str(obj)}

    # Extract results from GE validation
    for result in validation_result.results:
        result_dict = serialize(result.result if hasattr(result, 'result') else {})

        meta = result.expectation_config.meta or {}
        severity = meta.get("severity", "medium")

        results.append({
            "check": result.expectation_config.expectation_type,
            "description": str(result.expectation_config.kwargs),
            "severity": severity,
            "result": result_dict
        })

        if not result.success:
            errors.append({
                "type": result.expectation_config.expectation_type,
                "message": str(result.expectation_config.kwargs),
                "severity": severity,
                "details": result_dict
            })

            if severity_rank[severity] > severity_rank[criticality]:
                criticality = severity

    required = ["Age", "Gender", "Geography", "EstimatedSalary"]

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
            "ge_report_url": report_url
        }

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

    format_mask = df.apply(
        lambda col: col.astype(str).str.contains(r'^(ERR_|INVALID)', regex=True, na=False)
    ).any(axis=1)
    bad_mask |= format_mask

    numeric_cols = ["HasCrCard", "IsActiveMember", "NumOfProducts", "CreditScore", "Tenure", "Balance"]
    for col in numeric_cols:
        if col in df.columns:
            col_num = pd.to_numeric(df[col], errors="coerce")
            bad_mask |= col_num.isna()

    bad_idx = df.index[bad_mask].tolist()
    good_idx = df.index[~bad_mask].tolist()


    if len(bad_idx) > 0:
        errors.append({
            "type": "row_level_validation",
            "message": f"{len(bad_idx)} rows failed validation (duplicates, format errors, etc.)",
            "severity": "medium",
            "details": {
                "invalid_row_count": len(bad_idx),
                "total_rows": len(df),
                "checks": "duplicates, format_errors, data_type_errors"
            }
        })


    total_errors = len(errors)

    if total_errors >= 3:
        criticality = "high"
    elif total_errors == 2:
        criticality = "medium"
    else:  # 0 or 1 error
        criticality = "low"

    print(f" Valid: {len(good_idx)} |  Invalid: {len(bad_idx)}")
    print(f" Criticality: {criticality.upper()} (based on {total_errors} error(s))")
    print(f" Errors found: {total_errors}")
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