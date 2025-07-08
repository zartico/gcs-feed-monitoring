from datetime import date, timedelta
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../dags/raw_data_monitoring/src')))

from analyzer import analyze_feed
from bq_client import query_historical_baseline
from config import FEEDS, ALERT_RECIPIENTS

def make_file(size_mb, actual_date):
    return {"size": size_mb * 1_000_000, "actual_date": actual_date}

# Baseline match - return OK
def test_analyze_feed_ok(monkeypatch):
    # Baseline: 10 files, 100MB each, for 30 days
    today = date.today()
    baseline_day = today - timedelta(days=1)
    file_metadata = []
    for i in range(30):
        d = baseline_day - timedelta(days=i)
        file_metadata.extend([make_file(100, d) for _ in range(10)])
    # Today: 10 files, 100MB each
    file_metadata.extend([make_file(100, today) for _ in range(10)])

    # Patch query_historical_baseline to return baseline
    monkeypatch.setattr("analyzer.query_historical_baseline", lambda *a, **kw: (10, 1000))
    result = analyze_feed("TestFeed", file_metadata, today)
    assert result["status"] == "OK ✅"

# No data - return CRITICAL
def test_analyze_feed_critical(monkeypatch):
    today = date.today()
    file_metadata = []
    # Patch baseline
    monkeypatch.setattr("analyzer.query_historical_baseline", lambda *a, **kw: (10, 100))
    result = analyze_feed("TestFeed", file_metadata, today)
    assert result["status"] == "CRITICAL 🚨"
    assert "No data received." in result["issues"]

# File count deviates - return WARNING
def test_analyze_feed_warning_count(monkeypatch):
    today = date.today()
    file_metadata = []
    # Baseline: 10 files, 100MB each, for 30 days
    for i in range(30):
        d = today - timedelta(days=i+1)
        file_metadata.extend([make_file(100, d) for _ in range(10)])
    # Today: 5 files (50% less)
    file_metadata.extend([make_file(100, today) for _ in range(5)])
    monkeypatch.setattr("analyzer.query_historical_baseline", lambda *a, **kw: (10, 100))
    result = analyze_feed("TestFeed", file_metadata, today)
    assert result["status"] == "WARNING ❗️"
    assert any("File count deviates" in issue for issue in result["issues"])

# File size deviates - return WARNING
def test_analyze_feed_warning_size(monkeypatch):
    today = date.today()
    file_metadata = []
    # Baseline: 10 files, 100MB each, for 30 days
    for i in range(30):
        d = today - timedelta(days=i+1)
        file_metadata.extend([make_file(100, d) for _ in range(10)])
    # Today: 10 files, 50MB each (size deviation)
    file_metadata.extend([make_file(50, today) for _ in range(10)])
    monkeypatch.setattr("analyzer.query_historical_baseline", lambda *a, **kw: (10, 100))
    result = analyze_feed("TestFeed", file_metadata, today)
    assert result["status"] == "WARNING ❗️"
    assert any("Size deviates" in issue for issue in result["issues"])

# File count and size deviate - return WARNING
def test_analyze_feed_warning_both(monkeypatch):
    today = date.today()
    file_metadata = []
    # Baseline: 10 files, 100MB each
    for i in range(30):
        d = today - timedelta(days=i+1)
        file_metadata.extend([make_file(100, d) for _ in range(10)])
    # Today: 5 files, 40MB each
    file_metadata.extend([make_file(40, today) for _ in range(5)])
    monkeypatch.setattr("analyzer.query_historical_baseline", lambda *a, **kw: (10, 100))
    result = analyze_feed("TestFeed", file_metadata, today)
    assert result["status"] == "WARNING ❗️"
    assert len(result["issues"]) == 2
    assert any("File count deviates" in issue for issue in result["issues"])
    assert any("Size deviates" in issue for issue in result["issues"])