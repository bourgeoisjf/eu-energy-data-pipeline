import os
import requests
from datetime import datetime
from dotenv import load_dotenv

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
OUTPUT_DIR = "data/raw"

DOCUMENT_TYPE = "A75"  # Actual generation per unit
PROCESS_TYPE = "A16"  # Realised
IN_DOMAIN = "10YFR-RTE------C"  # France bidding zone

# Example period (UTC)
PERIOD_START = "202601270000"
PERIOD_END = "202601280000"


# ======================================================
# Helper functions
# ======================================================
def build_request_params():
    return {
        "securityToken": API_KEY,
        "documentType": DOCUMENT_TYPE,
        "processType": PROCESS_TYPE,
        "in_Domain": IN_DOMAIN,
        "periodStart": PERIOD_START,
        "periodEnd": PERIOD_END,
    }


def fetch_generation_data():
    response = requests.get(BASE_URL, params=build_request_params())

    if response.status_code != 200:
        raise RuntimeError(
            f"API request failed: {response.status_code} - {response.text}"
        )

    return response.text


def save_raw_response(content: str):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"entsoe_generation_fr_raw_{timestamp}.xml"
    filepath = os.path.join(OUTPUT_DIR, filename)

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)

    print(f"✅ Raw data saved to: {filepath}")


# ======================================================
# Main
# ======================================================
def main():
    print("🔌 Fetching generation data from ENTSO-E API...")
    data = fetch_generation_data()
    save_raw_response(data)
    print("🎉 Done!")


if __name__ == "__main__":
    main()
