import re
from datetime import date

def extract_actual_date(filename: str) -> date | None:
    """
    Extracts the actual date from a filename in the format 'impressions-v4-parquet/year=YYYY/month=MM/day=DD/filename'.
    Returns a date object or None if the date cannot be extracted.
    """
    if "$folder$" in filename:
        return None

    pattern = r"year=(\d{4})/month=(\d{2})/day=(\d{2})"
    match = re.search(pattern, filename)

    if match:
        try:
            year, month, day = map(int, match.groups())
            return date(year, month, day)
        except ValueError:
            # If the date is invalid, return None
            print(f"[!] Invalid date in path: {filename}")
            return None
    else:
        # If the pattern does not match, return None
        if not filename.endswith("/"): 
            print(f"[!] No date found in path: {filename}")
        return None
