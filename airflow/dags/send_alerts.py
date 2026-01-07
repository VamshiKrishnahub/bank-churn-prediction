import os
import requests
from pathlib import Path


def send_teams_alert(validation: dict, report_path: str):
    """
    Send Teams alert for data validation issues

    Args:
        validation: Dict containing validation results with 'errors' list
        report_path: URL to the HTML validation report (internal Docker URL)
    """

    webhook = os.environ.get("TEAMS_WEBHOOK")
    if not webhook:
        print(" No TEAMS_WEBHOOK configured — skipping alert.")
        return

    criticality = validation.get("criticality", "unknown").upper()
    errors = validation.get("errors", [])
    file_name = Path(validation["file_path"]).name

    # Convert internal Docker URL to external accessible URL
    # Replace 'fastapi:8000' with 'localhost:8000' for external access
    # If you have a public domain, replace localhost with your domain
    external_report_url = report_path.replace("http://fastapi:8000", "http://localhost:8000")

    # If you're deploying to a server with a public IP or domain, use this instead:
    # PUBLIC_HOST = os.getenv("PUBLIC_HOST", "localhost:8000")
    # external_report_url = report_path.replace("http://fastapi:8000", f"http://{PUBLIC_HOST}")

    print(f" Sending Teams alert")
    print(f"   Internal URL: {report_path}")
    print(f"   External URL: {external_report_url}")

    # Build error summary - handle both 'criticality' and 'severity' keys
    if errors:
        error_lines = []
        for e in errors:
            error_type = e.get('type', 'unknown')
            # Try 'severity' first (new format), fall back to 'criticality' (old format)
            severity = e.get('severity', e.get('criticality', 'unknown'))
            error_lines.append(f"- **{error_type}** ({severity})")
        error_summary = "\n".join(error_lines)
    else:
        error_summary = " No validation errors."

    # Determine color based on criticality
    if criticality == "HIGH":
        theme_color = "E81123"  # Red
    elif criticality == "MEDIUM":
        theme_color = "FF8C00"  # Orange
    else:
        theme_color = "0078D4"  # Blue

    message = {
        "@type": "MessageCard",
        "@context": "https://schema.org/extensions",
        "summary": f"Data Ingestion Alert - {criticality}",
        "themeColor": theme_color,
        "sections": [
            {
                "activityTitle": f" **Data Ingestion Alert — {criticality}**",
                "activitySubtitle": f"File: **{file_name}**",
            },
            {
                "text": f"###  Summary of Validation\n"
                        f"**Criticality:** {criticality}\n"
                        f"**Total Rows:** {validation.get('valid_rows', 0) + validation.get('invalid_rows', 0)}\n"
                        f"**Valid Rows:** {validation.get('valid_rows', 0)}\n"
                        f"**Invalid Rows:** {validation.get('invalid_rows', 0)}\n"
                        f"**Errors Found:** {len(errors)}\n\n"
                        f"###  Error Details\n"
                        f"{error_summary}",
            },
            {
                "text": f"[ **View Full Validation Report**]({external_report_url})"
            }
        ]
    }

    try:
        response = requests.post(webhook, json=message, timeout=10)

        if response.status_code == 200:
            print(" Teams alert sent successfully.")
        else:
            print(f" Error sending Teams alert: HTTP {response.status_code}")
            print(f"   Response: {response.text[:200]}")
    except Exception as e:
        print(f" Exception sending Teams alert: {e}")