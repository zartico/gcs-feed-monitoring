from gcs_client import list_gcs_metadata, group_by_date
from analyzer import analyze_feed
from datetime import date, timedelta

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

BUCKET_NAME = "web-impressions-raw"
PREFIX = "impressions-v4-parquet/"

def run():

    actual_date = date.today() - timedelta(days=6) 
    
    for key, feed in FEEDS.items():
        print(f"\nChecking feed: {feed['label']}")
        metadata = list_gcs_metadata(feed["bucket"], feed["prefix"], debug=False)
        result = analyze_feed(metadata, actual_date)

        print("\n===== ANALYSIS RESULT =====")
        print(f"Status: {result['status']}")
        print(f"Expected Date: {result['date']}")
        print(f"File Count: {result['file_count']} (Baseline: {result['monthly_avg_count']:.1f})")
        print(f"Size: {result['file_size_mb']:.2f} MB (Baseline: {result['monthly_avg_size_mb']:.2f} MB)")

        if result['issues']:
            print("Issues Detected:")
            for issue in result['issues']:
                print(f" - {issue}")
        else:
            print("No anomalies detected.")


if __name__ == "__main__":
    run()

