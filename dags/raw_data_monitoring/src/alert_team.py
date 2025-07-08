import requests
import json
from config import FEEDS, ALERT_RECIPIENTS, LOOKER_LINK
from tabulate import tabulate

def send_alert_to_team(webhook_url, alert_message):
    """   
    Sends an alert message to the team via a Slack webhook.
    
    Args:
        webhook_url (str): The Slack webhook URL to send the alert to.
        alert_message (str): The message to send as an alert.
    """

    headers = {'Content-Type': 'application/json'}
    payload = {
        "text": alert_message,
        "mrkdwn": True
        }

    response = requests.post(webhook_url, data=json.dumps(payload), headers=headers)
    
    if response.status_code != 200:
        print(f"Error sending message: {response.text}")

def format_overview_table(results: list[tuple[str, str, str]]) -> str:
    """
    Formats the results into a Markdown table.

    Args:
        results (list): A list of tuples containing feed label, status, and expected date.
    
    Returns:
        str: A Markdown formatted table as a string.
    """

    header = ["Feed", "Status", "Expected Date"]
    table = tabulate(results, headers=header, tablefmt="grid", stralign="left", numalign="left")
    looker_line = f"\n 📈 <{LOOKER_LINK}|View Historical Trends in Looker Studio>"
    return f"```\n{table}\n```{looker_line}"

def format_alert_details(feed_label, result):
    """    
    Formats the alert details for a specific feed into a Slack message.
    
    Args:
        feed_label (str): The label of the feed.
        result (dict): The analysis result containing status, date, file count, size, and issues.     
    
    Returns:
        str: A formatted string containing the alert details.
    """

    lines = [f"*Feed:* {FEEDS[feed_label]['label']}"]
    user_mentions = " ".join(f"<@{uid}>" for uid in ALERT_RECIPIENTS.get(feed_label, []))
    if user_mentions:
        lines.append(user_mentions)
    lines.append("\n===== ANALYSIS RESULT =====")
    lines.append(f"Status: {result['status']}")
    lines.append(f"Expected Date: {result['date']}")
    lines.append(f"File Count: {result['file_count']} (Baseline: {result['monthly_avg_count']:.1f})")
    lines.append(f"Size: {result['file_size_mb']:.2f} MB (Baseline: {result['monthly_avg_size_mb']:.2f} MB)")

    # Append details of issues if any
    if result['issues']:
       lines.append("Issues Detected:")
       for issue in result['issues']:
           lines.append(f" - {issue}")

    return "\n".join(lines)