from gcs_client import list_gcs_metadata, group_by_date
from analyzer import analyze_feed
from alert_team import send_alert_to_team
from config import FEEDS, ALERT_RECIPIENTS
from datetime import date, timedelta
from dotenv import load_dotenv
load_dotenv()  # Load environment variables from .env file
import os


SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

def format_result_as_message(feed_label, result):
    lines = [f"*Feed:* {feed_label}"]
    lines.append("\n===== ANALYSIS RESULT =====")
    lines.append(f"Status: {result['status']}")
    lines.append(f"Expected Date: {result['date']}")
    lines.append(f"File Count: {result['file_count']} (Baseline: {result['monthly_avg_count']:.1f})")
    lines.append(f"Size: {result['file_size_mb']:.2f} MB (Baseline: {result['monthly_avg_size_mb']:.2f} MB)")

    if result['issues']:
       lines.append("Issues Detected:")
       for issue in result['issues']:
           lines.append(f" - {issue}")
    if result["status"] == "CRITICAL":
        user_mentions = " ".join(f"<@{uid}>" for uid in ALERT_RECIPIENTS.get(feed_label, []))
        lines.append(user_mentions)
        lines.append(f"🚨 *CRITICAL ALERT for {feed_label}* 🚨\n{result}")

    else:
        lines.append("No anomalies detected.")

    return "\n".join(lines)

def run():

    actual_date = date.today() - timedelta(days=6) 
    
    for key, feed in FEEDS.items():
        print(f"\nChecking feed: {feed['label']}")
        metadata = list_gcs_metadata(feed["bucket"], feed["prefix"], debug=False)
        result = analyze_feed(metadata, actual_date)
    
        # Formatted daily report message sent to Slack
        message = format_result_as_message(feed["label"], result)
        print(message)
        send_alert_to_team(SLACK_WEBHOOK_URL, message)


if __name__ == "__main__":
    run()
