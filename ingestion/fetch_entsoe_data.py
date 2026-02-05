"""
ENTSO-E API Data Extraction Script.
This version supports dynamic date execution via Airflow and uses a
reference file for country mapping to ensure scalability.
"""

import os
import sys
import requests
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv

# ======================================================
# 1. PROJECT PATH & REFERENCE SETUP
# ======================================================
# Define the project root and add it to sys.path to allow internal imports
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

# Attempt to import the country mapping from your reference folder
try:
    from data.reference.countries import COUNTRIES
except ImportError:
    print("❌ CRITICAL: data/reference/countries.py not found!")
    sys.exit(1)

# ======================================================
# 2. CONFIGURATION & ENVIRONMENT
# ======================================================
# Load API keys and other secrets from the .env file
load_dotenv(PROJECT_ROOT / ".env")
API_KEY = os.getenv("ENTSOE_API_KEY")

if not API_KEY:
    raise ValueError("CRITICAL: ENTSOE_API_KEY not found in .env file.")

BASE_URL = "https://web-api.tp.entsoe.eu/api"

# ======================================================
# 3. DYNAMIC DATE HANDLING (AIRFLOW SUPPORT)
# ======================================================
# If an argument is passed (usually by Airflow), use it as the target date.
# Otherwise, default to yesterday's date for manual runs.
if len(sys.argv) > 1:
    # Expected format from Airflow: YYYY-MM-DD
    target_date = datetime.strptime(sys.argv[1], "%Y-%m-%d")
else:
    target_date = datetime.now() - timedelta(days=1)

# TIME WINDOW FIX:
# ENTSO-E API requires precise windows. We request from 00:00 of the target day
# until 00:00 of the following day to ensure a full 24-hour block is captured.
# This prevents Error 400 caused by incomplete time intervals.
PERIOD_START = target_date.strftime("%Y%m%d0000")
PERIOD_END = (target_date + timedelta(days=1)).strftime("%Y%m%d0000")

# Partitioning variables for folder structure
YEAR = target_date.strftime("%Y")
MONTH = target_date.strftime("%m")
DAY = target_date.strftime("%d")

# Define categories: A75 (Generation) and A44 (Day-Ahead Prices)
DATA_CONFIG = [
    {"doc_type": "A75", "folder": "generation", "process_type": "A16"},
    {"doc_type": "A44", "folder": "prices", "process_type": None},
]

# ======================================================
# 4. CORE FUNCTIONS
# ======================================================


def fetch_xml_from_api(bidding_zone, doc_type, process_type):
    """
    Handles the HTTP GET request to the ENTSO-E API.
    Includes specific logic for Prices (A44) which requires 'out_Domain'.
    """
    params = {
        "securityToken": API_KEY,
        "documentType": doc_type,
        "in_Domain": bidding_zone,
        "periodStart": PERIOD_START,
        "periodEnd": PERIOD_END,
    }

    # Generation documents (A75) require a processType
    if process_type:
        params["processType"] = process_type

    # Prices (A44) often require 'out_Domain' to match the bidding zone
    if doc_type == "A44":
        params["out_Domain"] = bidding_zone

    try:
        response = requests.get(BASE_URL, params=params, timeout=30)

        if response.status_code == 200:
            return response.text
        else:
            # Print error details to help debug issues like Timezone or API constraints
            print(f"    ⚠️ API Error {response.status_code} for zone {bidding_zone}")
            if response.status_code == 400:
                print(f"    Debug Message: {response.text[:150]}")
            return None

    except Exception as e:
        print(f"    ❌ Connection failed: {e}")
        return None


# ======================================================
# 5. MAIN PROCESS
# ======================================================


def main():
    print("--- ENTSO-E INGESTION PIPELINE ---")
    print(f"Target Date: {YEAR}-{MONTH}-{DAY}")
    print(f"Time Window: {PERIOD_START} to {PERIOD_END}")

    # Track overall success to notify Airflow if a critical failure occurs
    overall_success = True

    for config in DATA_CONFIG:
        doc_type = config["doc_type"]
        folder_name = config["folder"]
        process_type = config["process_type"]

        # Create partitioned directory: data/raw/{folder}/{YYYY}/{MM}/{DD}
        output_dir = PROJECT_ROOT / "data" / "raw" / folder_name / YEAR / MONTH / DAY
        output_dir.mkdir(parents=True, exist_ok=True)

        print(f"\n📂 CATEGORY: {folder_name.upper()}")

        success_count = 0
        # Iterate through all countries defined in the reference file
        for country_code, meta in COUNTRIES.items():
            bidding_zone = meta["bidding_zone"]
            country_name = meta["country_name"]

            xml_content = fetch_xml_from_api(bidding_zone, doc_type, process_type)

            if xml_content:
                file_name = f"{folder_name}_{country_code}.xml"
                file_path = output_dir / file_name

                # Save raw XML data with UTF-8 encoding
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(xml_content)

                print(f"    ✅ {country_name} ({country_code}) - Success")
                success_count += 1
            else:
                print(f"    ❌ {country_name} ({country_code}) - Failed")

        print(
            f"📊 Summary for {folder_name}: {success_count}/{len(COUNTRIES)} countries saved."
        )

        # If an entire category fails to download any data, mark the pipeline as failed
        if success_count == 0:
            overall_success = False

    # Airflow integration: exit with code 1 to trigger a 'failed' state in the UI
    if not overall_success:
        print("\n❌ FATAL: At least one category failed completely.")
        sys.exit(1)

    print("\n✨ INGESTION FINISHED SUCCESSFULLY")


if __name__ == "__main__":
    main()
