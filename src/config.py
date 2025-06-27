
FEEDS = {
    "geo": {
        "label": "Geolocation",
        "bucket": "geolocation_raw",
        "prefix": "location_data_expanded_parquet/region=USA",
    },
    "web": {
        "label": "Web Impressions",
        "bucket": "web-impressions-raw",
        "prefix": "impressions-v4-parquet/",
    },
    "media": {
        "label": "Media Impressions",
        "bucket": "dco-impressions-raw",
        "prefix": "marketing-v5-parquet/",
    }
}

ALERT_RECIPIENTS = {
    "geo": ["U03U54XCHB2", "U073WNX896U", "U01VC13DAUT", "U01FHQJMH9C"],    # John, Carson, Chris, Eddie
    "web": ["U01VC13DAUT", "U07A66M08CV", "U01FHQJMH9C"], # Chris, Alexis, Eddie
    "media": ["U01VC13DAUT", "U07A66M08CV", "U01FHQJMH9C"], # Chris, Alexis, Eddie
}

PROJECT_ID = "prj-prod-gd-api-load-t51k"
DATASET_ID = "AziraMonitoring"
TABLE_ID = "raw_data_monitoring"