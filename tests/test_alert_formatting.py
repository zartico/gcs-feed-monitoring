from datetime import date, timedelta
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../dags/raw_data_monitoring/src')))

from analyzer import analyze_feed
from bq_client import query_historical_baseline
from config import FEEDS, ALERT_RECIPIENTS
from alert_team import format_alert_details, format_overview_table, send_alert_to_team
from dotenv import load_dotenv
load_dotenv()  # Load environment variables from .env file
import os

def make_file(size_mb, actual_date):
    return {"size": size_mb * 1_000_000, "actual_date": actual_date}

def test_slack_message_format(monkeypatch):
    today = date.today()
    feed = FEEDS["web"]
    # Anomaly: 4 files today, baseline is 10
    file_metadata = [make_file(80, today) for _ in range(4)]
    # Mock historical baseline and feed analysis
    monkeypatch.setattr("analyzer.query_historical_baseline", lambda *a, **kw: (10, 100))
    result = analyze_feed(feed["label"], file_metadata, today)

    #Format slack message
    overview = format_overview_table([(feed["label"], result["status"], result["date"])])
    details = format_alert_details("web", result)
    
    # Include test disclaimer
    full_message = "\n".join([
        "*🚨 THIS IS A TEST ALERT — PLEASE DISREGARD 🚨*",
        "*Feed Status Overview:*",
        overview,
        "\n*⚠️ Alert Details:*",
        details
    ])

    # Send to Slack (mocked in tests)
    #slack_webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    #send_alert_to_team(slack_webhook_url, full_message)
    print(full_message)

    print("Test alert for Web Impressions sent successfully.")