# utils/send_alerts.py

import requests
import pandas as pd
from sqlalchemy import create_engine
import os
from datetime import datetime, timedelta


# ============================================================
# CONFIG
# ============================================================

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://admin:admin@db:5432/defence_db"
)

TEAMS_WEBHOOK_URL = os.getenv(
    "TEAMS_WEBHOOK_URL",
    "https://your-teams-webhook-url"   # <-- replace in .env
)


# ============================================================
# QUERY LAST INGESTION ERRORS
# ============================================================

def fetch_recent_issues(minutes: int = 10) -> pd.DataFrame:
    """
    Fetch all data validation issues from last X minutes.
    """

    engine = create_engine(DATABASE_URL)

    query = f"""
        SELECT *
        FROM data_quality_issues
        WHERE created_at >= NOW() - INTERVAL '{minutes} MINUTES'
        ORDER BY created_at DESC;
    """

    try:
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        print("❌ Failed to query DB:", e)
        return pd.DataFrame()


# ============================================================
# FORMAT ALERT MESSAGE FOR TEAMS
# ============================================================

def build_alert_message(df: pd.DataFrame) -> str:
    """
    Format a message summarizing validation issues.
    """

    if df.empty:
        return (
            "✅ *No data quality issues detected in the last ingestion.*"
        )

    total = len(df)
    by_type = df["error_type"].value_counts().to_dict()

    msg = (
        f"🚨 **Data Quality Alert — Latest Ingestion**\n\n"
        f"**Total Issues:** {total}\n\n"
        "**Breakdown:**\n"
    )

    for k, v in by_type.items():
        msg += f"- {k}: {v}\n"

    msg += (
        "\n🕒 Time: "
        + datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    )

    return msg


# ============================================================
# SEND MESSAGE TO TEAMS
# ============================================================

def send_to_teams(message: str):
    """
    Send formatted message to Microsoft Teams.
    """

    if not TEAMS_WEBHOOK_URL or "http" not in TEAMS_WEBHOOK_URL:
        print("⚠️  No valid Teams webhook configured — skipping alert.")
        return

    payload = {"text": message}

    try:
        response = requests.post(TEAMS_WEBHOOK_URL, json=payload)
        response.raise_for_status()
        print("📨 Teams notification sent successfully.")
    except Exception as e:
        print("❌ Failed to send Teams alert:", e)


# ============================================================
# MAIN ENTRY FOR AIRFLOW
# ============================================================

def send_alerts(**context):
    """
    Airflow task entry:
    - fetch issues from DB
    - build notification
    - send to Teams ONLY if issues exist
    """

    print("🔍 Fetching data quality issues...")

    issues = fetch_recent_issues(minutes=10)

    print(f"🔎 Found {len(issues)} issues in last 10 minutes")

    message = build_alert_message(issues)

    # Only alert if issues exist
    if not issues.empty:
        send_to_teams(message)
    else:
        print("✔ No issues found — no alert sent.")

    return "Alert process complete."
