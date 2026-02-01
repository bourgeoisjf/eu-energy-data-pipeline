import os
import sys
import requests
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# ======================================================
# Project root & imports
# ======================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from data.reference.countries import COUNTRIES

# ======================================================
# Environment variables
# ======================================================

load_dotenv(PROJECT_ROOT / ".env")

API_KEY = os.getenv("ENTSOE_API_KEY")

if not API_KEY:
    raise ValueError("ENTSOE_API_KEY not found. Check your .env file.")

# ======================================================
# Constants
# ======================================================

BASE_URL = "https://web-api.tp.entsoe.eu/api"

DOCUMENT_TYPE = "A75"  # Actual generation per unit
PROCESS_TYPE = "A16"  # Realised

PERIOD_START = "202601270000"
PERIOD_END = "202601280000"

RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw" / "generation"
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

# ======================================================
# Helper functions
# ======================================================


def fetch_generation_xml(bidding_zone: str) -> str:
    """Fetch raw generation XML from ENTSO-E for a given bidding zone."""

    params = {
        "securityToken": API_KEY,
        "documentType": DOCUMENT_TYPE,
        "processType": PROCESS_TYPE,
        "in_Domain": bidding_zone,
        "periodStart": PERIOD_START,
        "periodEnd": PERIOD_END,
    }

    response = requests.get(BASE_URL, params=params)

    if response.status_code != 200:
        raise RuntimeError(
            f"API request failed for {bidding_zone}: "
            f"{response.status_code} - {response.text}"
        )

    return response.text


# ======================================================
# Main
# ======================================================


def main():
    print("🌍 Starting multi-country ENTSO-E ingestion...")

    for country_code, meta in COUNTRIES.items():
        bidding_zone = meta["bidding_zone"]
        country_name = meta["country_name"]

        print(f"📥 Fetching data for {country_name} ({country_code})")

        xml_content = fetch_generation_xml(bidding_zone)

        output_file = RAW_DATA_DIR / f"generation_{country_code}.xml"

        with open(output_file, "w", encoding="utf-8") as f:
            f.write(xml_content)

        print(f"✅ Saved raw XML to {output_file}")

    print("🎉 Ingestion completed for all countries.")


if __name__ == "__main__":
    main()
