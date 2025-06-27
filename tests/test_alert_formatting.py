from datetime import date, timedelta
from src.analyzer import analyze_feed
from src.bq_client import query_historical_baseline
from src.config import FEEDS, ALERT_RECIPIENTS
from src.alert_team import format_alert_details, format_overview_table

def make_file(size_mb, actual_date):
    return {"size": size_mb * 1_000_000, "actual_date": actual_date}

def test_slack_message_format(monkeypatch):
    today = date.today()
    # Fake metadata: 5 files today, baseline is 10
    file_metadata = [make_file(100, today) for _ in range(5)]
    monkeypatch.setattr("src.analyzer.query_historical_baseline", lambda *a, **kw: (10, 100))
    result = analyze_feed("Geolocation", file_metadata, today)
    overview = format_overview_table([("Geolocation", result["status"], result["date"])])
    details = format_alert_details("Geolocation", result)
    print("=== Overview Table ===")
    print(overview)
    print("=== Alert Details ===")
    print(details)