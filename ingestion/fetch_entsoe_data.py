import os
import requests
from datetime import datetime
from dotenv import load_dotenv

from data.reference.countries import COUNTRIES

# ======================================================
# Load environment variables
# ======================================================
load_dotenv()

API_KEY = os.getenv("ENTSOE_API_KEY")

if not API_KEY:
    raise ValueError("ENTSOE_API_KEY not found. Check your .env file.")

# ======================================================
# Constants
# ======================================================
BASE_URL = "https://web-api.tp.entsoe.eu/api"
OUTPUT_DIR = "data/raw/generation"

DOCUMENT_TYPE = "A75"   # Actual generation per type
PROCESS_TYPE = "A16"    # Realised

# Example period (UTC)
PERIOD_START = "202601270000"
PERIOD_END = "202601280000"

# ======================================================
# Helper functions
# ======================================================
def build_request_params(bidding_zone: str) -> dict:
    """
    Build request parameters for the ENTSO-E API.

    Parameters
    ----------
    bidding_zone : str
        ENTSO-E bidding zone code (e.g. France, Germany, Spain)

    Returns
    -------
    dict
        Dictionary with API request parameters
    """
    return {
        "securityToken": API_KEY,
        "documentType": DOCUMENT_TYPE,
        "processType": PROCESS_TYPE,
        "in_Domain": bidding_zone,
        "periodStart": PERIOD_START,
        "periodEnd": PERIOD_END,
    }


def fetch_generation_data(bidding_zone: str) -> str:
    """
    Fetch generation data XML from ENTSO-E API for a given bidding zone.
    """
    response = requests.get(
        BASE_URL,
        params=build_request_params(bidding_zone),
    )

    if response.status_code != 200:
        raise RuntimeError(
            f"API request failed ({bidding_zone}): "
            f"{response.status_code} - {response.text}"
        )

    return response.text


def save_raw_response(content: str, country_code: str) -> None:
    """
    Save raw XML response to disk with country-specific filename.
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"entsoe_generation_{country_code}_raw_{timestamp}.xml"
    filepath = os.path.join(OUTPUT_DIR, filename)

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)

    print(f"✅ Raw data saved to: {filepath}")

# ======================================================
# Main
# ======================================================
def main():
    print("🌍 Starting multi-country ENTSO-E ingestion...")

    for country_code, meta in COUNTRIES.items():
        country_name = meta["country_name"]
        bidding_zone = meta["bidding_zone"]

        print(f"📥 Fetching data for {country_name} ({country_code})")

        xml_content = fetch_generation_data(bidding_zone)
        save_raw_response(xml_content, country_code)

    print("🎉 Ingestion completed for all countries.")


if __name__ == "__main__":
    main()
