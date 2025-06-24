from datetime import datetime, date, timedelta
from collections import defaultdict

def calculate_baseline(historical_data: dict[date, list[dict]]) -> tuple[float, float]:
    
    today = date.today()
    past_month = [today - timedelta(days=i + 6) for i in range(30)]

    baseline_count = []
    baseline_file_size = []

    for day in past_month:
        if day in historical_data:
            daily_data = historical_data[day]
            baseline_count.append(len(daily_data))
            baseline_file_size.append(sum(file["size"] for file in daily_data) / 1_000_000)
    
    # Calculate averages, catch for no data
    avg_count = sum(baseline_count) / len(baseline_count) if baseline_count else 0.0
    avg_size_mb = sum(baseline_file_size) / len(baseline_file_size) if baseline_file_size else 0.0
    
    return avg_count, avg_size_mb

def analyze_feed(file_metadata: list[dict], expected_date: date) -> dict:

    # Group files by their actual date
    files_by_date = defaultdict(list)
    for file in file_metadata:
        actual_date = file.get("actual_date")
        if actual_date:
            files_by_date[actual_date].append(file)

    # Historical data for the last 30 days
    avg_count, avg_size_mb = calculate_baseline(files_by_date)
    
    # Today's file delivery
    today_files = files_by_date.get(expected_date, [])
    today_count = len(today_files)
    today_size_mb = sum(file["size"] for file in today_files) / 1_000_000 if today_files else 0.0

    
    # Baseline comparison
    status = "OK ✅"
    issues = []

    if today_count == 0 or today_size_mb == 0:
        status = "CRITICAL 🚨"
        issues.append("No data received.")
    
    else:
        if avg_count > 0 and abs(today_count - avg_count) / avg_count > 0.25:
            issues.append(f"File count deviates ≥25%: {today_count} vs baseline {avg_count:.1f}")
            status = "WARNING ❗️"

        if avg_size_mb > 0 and abs(today_size_mb - avg_size_mb) / avg_size_mb > 0.25:
            issues.append(f"Size deviates ≥25%: {today_size_mb:.2f} MB vs baseline {avg_size_mb:.2f} MB")
            if status != "CRITICAL 🚨":
                status = "WARNING ❗️"

    return {
        "status": status,
        "date" : expected_date.strftime("%Y-%m-%d"),
        "file_count": today_count,
        "file_size_mb": today_size_mb,
        "monthly_avg_count": avg_count,
        "monthly_avg_size_mb": avg_size_mb,
        "issues": issues
    }


