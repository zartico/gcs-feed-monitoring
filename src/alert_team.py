import requests
import json

def send_alert_to_team(webhook_url, alert_message):
    headers = {'Content-Type': 'application/json'}
    payload = {"text": alert_message}

    response = requests.post(webhook_url, data=json.dumps(payload), headers=headers)
    
    if response.status_code != 200:
        print(f"Error sending message: {response.text}")


