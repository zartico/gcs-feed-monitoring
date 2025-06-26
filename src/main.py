from gcs_client import list_gcs_metadata
from bq_client import upsert_feed_metrics
from analyzer import analyze_feed
from alert_team import send_alert_to_team, format_overview_table, format_alert_details
from config import FEEDS, ALERT_RECIPIENTS
from datetime import date, timedelta
from collections import defaultdict
from dotenv import load_dotenv
load_dotenv()  # Load environment variables from .env file
import os


SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

def run():

    actual_date = date.today() - timedelta(days=6) 
    overview_rows = []
    alert_messages = []
    
    for key, feed in FEEDS.items():
        print(f"\nChecking feed: {feed['label']}")
        metadata = list_gcs_metadata(feed["bucket"], feed["prefix"], debug=False)
        result = analyze_feed(feed["label"], metadata, actual_date)

        # Upsert metrics to BigQuery
        # upsert_feed_metrics(
        #     feed_label=feed["label"],
        #     event_date=actual_date,
        #     file_count=result["file_count"],
        #     file_size=result["file_size_mb"]
        # )

        # Append to Slack overview table
        overview_rows.append((feed["label"], result["status"], result["date"]))

        # Append detailed alert messages if status is CRITICAL or WARNING
        if result["status"] in ["CRITICAL 🚨", "WARNING ❗️"]:
            alert_messages.append(format_alert_details(feed["label"], result))

         
    # Slack message construction
    message_parts = []
    message_parts.append("*Feed Status Overview:*")
    message_parts.append(format_overview_table(overview_rows))

    if alert_messages:
        message_parts.append("\n*⚠️ Alerts:*")
        message_parts.extend(alert_messages)

    full_message = "\n".join(message_parts)
    print(full_message)
    #send_alert_to_team(SLACK_WEBHOOK_URL, full_message)

if __name__ == "__main__":
    run()
