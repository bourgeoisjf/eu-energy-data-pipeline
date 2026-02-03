"""
ENTSO-E API Data Extraction Script.
Updated to accept dynamic dates from Airflow.
"""

import os
import sys
import requests
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv

# 1. SETUP PATHS & ENV
PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

API_KEY = os.getenv("ENTSOE_API_KEY")
BASE_URL = "https://web-api.tp.entsoe.eu/api"

# 2. DYNAMIC DATE HANDLING
if len(sys.argv) > 1:
    # Airflow passes date as YYYY-MM-DD
    execution_date = datetime.strptime(sys.argv[1], "%Y-%m-%d")
else:
    # Fallback to yesterday if run manually without args
    execution_date = datetime.now() - timedelta(days=1)

# ENTSO-E needs a range. We want the full day of the execution_date
start_str = execution_date.strftime("%Y%m%d0000")
end_str = execution_date.strftime("%Y%m%d2300")
folder_path = execution_date.strftime("%Y/%m/%d")

# 3. COUNTRIES TO FETCH
COUNTRIES = {
    "10YFR-RTE------C": "FR",
    "10YES-REE------0": "ES",
    "10YPT-REN------W": "PT",
    "10YDE-VE-------2": "DE",
}


def download_data(doc_type, category):
    print(f"🚀 Starting download for {category} | Period: {start_str} to {end_str}")

    for domain, code in COUNTRIES.items():
        params = {
            "securityToken": API_KEY,
            "documentType": doc_type,
            "processType": "A16",
            "in_Domain": domain,
            "periodStart": start_str,
            "periodEnd": end_str,
        }

        response = requests.get(BASE_URL, params=params)

        if response.status_code == 200:
            # Create folder structure: data/raw/category/YYYY/MM/DD
            out_dir = PROJECT_ROOT / "data" / "raw" / category / folder_path
            out_dir.mkdir(parents=True, exist_ok=True)

            file_name = f"{category}_{code}.xml"
            with open(out_dir / file_name, "wb") as f:
                f.write(response.content)
            print(f"  ✅ Saved: {code}")
        else:
            print(
                f"  ❌ Error {response.status_code} for {code}: {response.text[:100]}"
            )


def main():
    if not API_KEY:
        print("❌ API Key not found! Check your .env file.")
        return

    # A75 = Generation, A44 = Prices
    download_data("A75", "generation")
    download_data("A44", "prices")
    print(f"\n✨ Ingestion complete for date: {folder_path}")


if __name__ == "__main__":
    main()
