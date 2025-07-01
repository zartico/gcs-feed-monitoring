# =========================
# CONFIGURATION TEMPLATE
# =========================
# Rename this file to `config.py` and fill in your actual values.

# Slack alert recipients (Slack User IDs)
ALERT_RECIPIENTS = {
    "Geolocation": ["U12345678"],          # Replace with actual Slack user ID(s)
    "Web Impressions": ["U23456789"],
    "Media Impressions": ["U34567890"],
}

# Data feeds configuration
FEEDS = {
    "geo": {
        "label": "Geolocation",
        "bucket": "your-gcs-geo-bucket-name",
        "prefix": "path/to/geo/files/",
    },
    "web": {
        "label": "Web Impressions",
        "bucket": "your-gcs-web-bucket-name",
        "prefix": "path/to/web/files/",
    },
    "media": {
        "label": "Media Impressions",
        "bucket": "your-gcs-media-bucket-name",
        "prefix": "path/to/media/files/",
    },
}

# BigQuery project and table info
PROJECT_ID = "your-gcp-project-id"
DATASET_ID = "your_dataset_name"
TABLE_ID = "your_table_name"

