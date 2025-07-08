from gcs_client import list_gcs_metadata
from bq_client import upsert_feed_metrics
from analyzer import analyze_feed
from alert_team import send_alert_to_team, format_overview_table, format_alert_details
from config import FEEDS, ALERT_RECIPIENTS
from datetime import date, timedelta
from collections import defaultdict
import os


# Run takes in slack webhook as arg, if none fetch from env variables, set default to NONE
def run(slack_webhook=None):

    if slack_webhook is None:
        from dotenv import load_dotenv
        load_dotenv()  # Load environment variables from .env file
        slack_webhook = os.getenv("SLACK_WEBHOOK_URL")

    actual_date = date.today() - timedelta(days=6) # Analysis target date
    upsert_start = date.today() - timedelta(days=13)  # Start date for upsert
    upsert_end = actual_date # End date for upsert
    overview_rows = []
    alert_messages = []
    

    # Single-day analysis for each feed
    for key, feed in FEEDS.items():
        print(f"\nChecking feed: {feed['label']}")
        metadata = list_gcs_metadata(feed["bucket"], feed["prefix"], debug=False)

        upsert_date = upsert_start
        # Upsert metrics for rolling 7 day window
        while upsert_date <= upsert_end:
            daily_files = [f for f in metadata if f.get("actual_date") == upsert_date]
            file_count_res = len(daily_files)
            file_size_res = sum(f["size"] for f in daily_files) / 1_000_000 if daily_files else 0.0

            # Upsert metrics to BigQuery
            upsert_feed_metrics(
                feed_label=feed["label"],
                event_date=upsert_date,
                file_count=file_count_res,
                file_size=file_size_res
            )

            upsert_date += timedelta(days=1)

        result = analyze_feed(feed["label"], metadata, actual_date)

        # Append to Slack overview table
        overview_rows.append((feed["label"], result["status"], result["date"]))

        # Append detailed alert messages if status is CRITICAL or WARNING
        if result["status"] in ["CRITICAL 🚨", "WARNING ❗️"]:
            alert_messages.append(format_alert_details(key, result))

         
    # Slack message construction
    message_parts = []
    message_parts.append("*Feed Status Overview:*")
    message_parts.append(format_overview_table(overview_rows))

    if alert_messages:
        message_parts.append("\n*⚠️ Alerts:*")
        message_parts.extend(alert_messages)

    full_message = "\n".join(message_parts)
    print(full_message)

    if slack_webhook:
        send_alert_to_team(slack_webhook, full_message)

if __name__ == "__main__":
    run()
