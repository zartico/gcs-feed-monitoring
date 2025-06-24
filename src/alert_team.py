import requests
import json
from config import FEEDS, ALERT_RECIPIENTS
from tabulate import tabulate

def send_alert_to_team(webhook_url, alert_message):
    headers = {'Content-Type': 'application/json'}
    payload = {"text": alert_message}

    response = requests.post(webhook_url, data=json.dumps(payload), headers=headers)
    
    if response.status_code != 200:
        print(f"Error sending message: {response.text}")

def format_overview_table(results: list[tuple[str, str, str]]) -> str:
    """
    Formats the results into a Markdown table.
    """
    header = ["Feed", "Status", "Expected Date"]
    table = tabulate(results, headers=header, tablefmt="grid", stralign="left", numalign="left")
    return f"```\n{table}\n```"

def format_alert_details(feed_label, result):
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
    if result["status"] == "CRITICAL 🚨":
        user_mentions = " ".join(f"<@{uid}>" for uid in ALERT_RECIPIENTS.get(feed_label, []))
        lines.append(user_mentions)
        lines.append(f"🚨 *CRITICAL ALERT for {feed_label}* 🚨\n{result}")

    else:
        lines.append("No anomalies detected.")

    return "\n".join(lines)