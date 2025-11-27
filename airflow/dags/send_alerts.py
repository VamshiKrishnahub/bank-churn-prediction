import os
import requests
from pathlib import Path


def send_teams_alert(validation: dict, report_path: str):
    """
    Sends Teams alert with CORRECT clickable HTTP link.
    """

    webhook = os.environ.get("TEAMS_WEBHOOK")
    if not webhook:
        print("❌ No TEAMS_WEBHOOK configured — skipping alert.")
        return

    criticality = validation.get("criticality", "unknown").upper()
    errors = validation.get("errors", [])
    file_name = Path(validation["file_path"]).name

    # ---------------------------------------------------
    # IMPORTANT: Build a REAL HTTP URL
    # ---------------------------------------------------
    # FastAPI serves reports at:
    # http://localhost:8000/reports/<filename>
    report_filename = Path(report_path).name
    report_url = f"http://localhost:8000/reports/{report_filename}"
    print("Generated Report URL:", report_url)

    # ---------------------------------------------------
    # Build error summary
    # ---------------------------------------------------
    if errors:
        error_summary = "\n".join(
            f"- **{e['type']}** ({e['criticality']})" for e in errors
        )
    else:
        error_summary = "No validation errors."

    # ---------------------------------------------------
    # Teams message payload
    # ---------------------------------------------------
    message = {
        "@type": "MessageCard",
        "@context": "https://schema.org/extensions",
        "summary": f"Data Ingestion Alert - {criticality}",
        "themeColor": "E81123",
        "sections": [
            {
                "activityTitle": f" **Data Ingestion Alert — {criticality}**",
                "activitySubtitle": f"File: **{file_name}**",
            },
            {
                "text": f"###  Summary of Validation\n"
                        f"**Criticality:** {criticality}\n"
                        f"**Errors:** {len(errors)}\n\n"
                        f"###  Error Details\n"
                        f"{error_summary}",
            },
            {
                "text": f"[ **Open HTML Validation Report**]({report_url})"
            }
        ]
    }

    response = requests.post(webhook, json=message)

    if response.status_code == 200:
        print(" Teams alert sent successfully.")
    else:
        print(" Error sending Teams alert:", response.status_code, response.text)
