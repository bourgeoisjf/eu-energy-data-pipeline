"""
Script: fetch_entsoe_data.py
Author: Jean-François Bourgeois
Purpose:
    Fetch raw electricity generation data from ENTSO-E API
    and store it locally as raw data.

This script is intentionally simple.
No transformations, no Spark, no Docker.
Pure ingestion.
"""

import os
import requests
from datetime import datetime


# =========================
# Configuration
# =========================

ENTSOE_API_KEY = os.getenv("ENTSOE_API_KEY")  # API key via environment variable
BASE_URL = "https://web-api.tp.entsoe.eu/api"

COUNTRY_CODE = "FR"  # France
DOCUMENT_TYPE = "A75"  # Actual generation per type
PERIOD_START = "202401010000"
PERIOD_END = "202401020000"

OUTPUT_DIR = "data/raw"


# =========================
# Helper functions
# =========================

def build_request_params():
    """Build query parameters for the API request."""
    return {
        "securityToken": ENTSOE_API_KEY,
        "documentType": DOCUMENT_TYPE,
        "processType": "A16",
        "in_Domain": "10YFR-RTE------C",
        "periodStart": PERIOD_START,
        "periodEnd": PERIOD_END,
    }


def fetch_data(params):
    """Fetch data from ENTSO-E API."""
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    return response.text


def save_raw_data(data):
    """Save raw XML data to disk."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"entsoe_generation_fr_{timestamp}.xml"
    filepath = os.path.join(OUTPUT_DIR, filename)

    with open(filepath, "w", encoding="utf-8") as file:
        file.write(data)

    print(f"Raw data saved to {filepath}")


# =========================
# Main execution
# =========================

def main():
    if not ENTSOE_API_KEY:
        raise ValueError("ENTSOE_API_KEY environment variable is not set")

    params = build_request_params()
    raw_data = fetch_data(params)
    save_raw_data(raw_data)


if __name__ == "__main__":
    main()
