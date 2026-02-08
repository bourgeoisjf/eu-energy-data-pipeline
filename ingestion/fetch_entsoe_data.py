"""
ENTSO-E API Data Extraction Script - UPDATED.
Now supports dual-zone mapping (Generation vs Prices) for countries like DE and IT.
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
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

try:
    from data.reference.countries import COUNTRIES
except ImportError:
    print("❌ CRITICAL: data/reference/countries.py not found!")
    sys.exit(1)

# ======================================================
# 2. CONFIGURATION & ENVIRONMENT
# ======================================================
load_dotenv(PROJECT_ROOT / ".env")
API_KEY = os.getenv("ENTSOE_API_KEY")

if not API_KEY:
    raise ValueError("CRITICAL: ENTSOE_API_KEY not found in .env file.")

BASE_URL = "https://web-api.tp.entsoe.eu/api"

# ======================================================
# 3. DYNAMIC DATE HANDLING
# ======================================================
if len(sys.argv) > 1:
    target_date = datetime.strptime(sys.argv[1], "%Y-%m-%d")
else:
    target_date = datetime.now() - timedelta(days=1)

PERIOD_START = target_date.strftime("%Y%m%d0000")
PERIOD_END = (target_date + timedelta(days=1)).strftime("%Y%m%d0000")

YEAR = target_date.strftime("%Y")
MONTH = target_date.strftime("%m")
DAY = target_date.strftime("%d")

DATA_CONFIG = [
    {
        "doc_type": "A75",
        "folder": "generation",
        "process_type": "A16",
        "zone_key": "gen_zone",
    },
    {
        "doc_type": "A44",
        "folder": "prices",
        "process_type": None,
        "zone_key": "price_zone",
    },
]

# ======================================================
# 4. CORE FUNCTIONS
# ======================================================


def fetch_xml_from_api(bidding_zone, doc_type, process_type):
    """
    Handles the HTTP GET request to the ENTSO-E API.
    """
    params = {
        "securityToken": API_KEY,
        "documentType": doc_type,
        "in_Domain": bidding_zone,
        "periodStart": PERIOD_START,
        "periodEnd": PERIOD_END,
    }

    if process_type:
        params["processType"] = process_type

    # Prices (A44) require 'out_Domain' to match the bidding zone for market clearing
    if doc_type == "A44":
        params["out_Domain"] = bidding_zone

    try:
        response = requests.get(BASE_URL, params=params, timeout=30)
        if response.status_code == 200:
            return response.text
        else:
            print(f"    ⚠️ API Error {response.status_code} for zone {bidding_zone}")
            return None
    except Exception as e:
        print(f"    ❌ Connection failed: {e}")
        return None


# ======================================================
# 5. MAIN PROCESS
# ======================================================


def main():
    print("--- ENTSO-E INGESTION PIPELINE (MULTI-ZONE VERSION) ---")
    print(f"Target Date: {YEAR}-{MONTH}-{DAY}")

    overall_success = True

    for config in DATA_CONFIG:
        doc_type = config["doc_type"]
        folder_name = config["folder"]
        process_type = config["process_type"]
        zone_key = config["zone_key"]  # Identify if we use gen_zone or price_zone

        output_dir = PROJECT_ROOT / "data" / "raw" / folder_name / YEAR / MONTH / DAY
        output_dir.mkdir(parents=True, exist_ok=True)

        print(f"\n📂 CATEGORY: {folder_name.upper()} (Using {zone_key})")

        success_count = 0
        for country_code, meta in COUNTRIES.items():
            # BUSCA O CÓDIGO DINAMICAMENTE BASEADO NO TIPO DE DADO
            bidding_zone = meta[zone_key]
            country_name = meta["country_name"]

            xml_content = fetch_xml_from_api(bidding_zone, doc_type, process_type)

            if xml_content:
                file_name = f"{folder_name}_{country_code}.xml"
                file_path = output_dir / file_name
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(xml_content)
                print(f"    ✅ {country_name} ({country_code}) - Success")
                success_count += 1
            else:
                print(
                    f"    ❌ {country_name} ({country_code}) - Failed (Zone: {bidding_zone})"
                )

        print(f"📊 Summary for {folder_name}: {success_count}/{len(COUNTRIES)} saved.")
        if success_count == 0:
            overall_success = False

    if not overall_success:
        print("\n❌ FATAL: At least one category failed completely.")
        sys.exit(1)

    print("\n✨ INGESTION FINISHED SUCCESSFULLY")


if __name__ == "__main__":
    main()
