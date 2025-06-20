import pandas as pd
import re

async def get_google_sheet_as_dataframe(sheet_url):
    """
    Convert a Google Sheet URL to a pandas DataFrame

    Args:
        sheet_url (str): Google Sheets URL (either sharing link or direct link)

    Returns:
        pandas.DataFrame: DataFrame containing the sheet data
    """

    # Extract the sheet ID from the URL
    sheet_id = extract_sheet_id(sheet_url)

    if not sheet_id:
        raise ValueError("Could not extract sheet ID from URL. Please check the URL format.")

    # Convert to CSV export URL
    csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"

    try:
        # Read the CSV data directly into pandas
        df = pd.read_csv(csv_url)
        return df

    except Exception as e:
        print(f"Error reading Google Sheet: {e}")
        print("Make sure the Google Sheet is publicly accessible or shared with viewing permissions.")
        return None


def extract_sheet_id(url):
    """
    Extract the Google Sheet ID from various URL formats

    Args:
        url (str): Google Sheets URL

    Returns:
        str: Sheet ID or None if not found
    """

    # Pattern to match Google Sheets ID
    patterns = [
        r'/spreadsheets/d/([a-zA-Z0-9-_]+)',  # Standard format
        r'id=([a-zA-Z0-9-_]+)',  # Alternative format
    ]

    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)

    return None