import xml.etree.ElementTree as ET
import pandas as pd
from pathlib import Path
import sys

# ======================================================
# 1. PATHS CONFIGURATION
# ======================================================
# Define the project root and subdirectories for raw and processed data
PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_BASE_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_BASE_DIR = PROJECT_ROOT / "data" / "processed"

# Define the data categories to be processed
DATA_TYPES = ["generation", "prices"]

# ======================================================
# 2. PARSING LOGIC
# ======================================================


def parse_xml_to_records(xml_path: Path, data_type: str) -> list[dict]:
    """
    Parses ENTSO-E XML files into a list of dictionaries.
    Handles differences between Generation (quantity) and Price (price.amount) tags.
    """
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()

        # Extract country code from filename (e.g., prices_DE.xml -> DE)
        # This keeps the 'DE' or 'IT' label regardless of the technical bidding zone code
        country_code = xml_path.stem.split("_")[-1]
        records = []

        # Iterate through each TimeSeries block in the XML
        for timeseries in root.findall(".//{*}TimeSeries"):
            # Identify the bidding zone from the XML tags
            bz_el = timeseries.find(".//{*}in_Domain.mRID") or timeseries.find(
                ".//{*}out_Domain.mRID"
            )
            bidding_zone = bz_el.text if bz_el is not None else "Unknown"

            # Extract the PSR Type (e.g., B01 for Biomass, B16 for Solar)
            psr_el = timeseries.find(".//{*}psrType")
            psr_type = psr_el.text if psr_el is not None else "N/A"

            # Extract period information
            period_el = timeseries.find(".//{*}Period")
            if period_el is None:
                continue

            start_time = period_el.find(".//{*}start").text
            resolution = period_el.find(".//{*}resolution").text

            # Iterate through each data point (usually hourly or 15min intervals)
            for point in period_el.findall(".//{*}Point"):
                pos = point.find("{*}position").text

                # Determine which value tag to look for based on data type
                val_el = (
                    point.find("{*}price.amount")
                    if data_type == "prices"
                    else point.find("{*}quantity")
                )

                if val_el is not None:
                    records.append(
                        {
                            "country": country_code,  # Administrative code (e.g., DE)
                            "bidding_zone": bidding_zone,  # Technical EIC code (e.g., 10Y1001A1001A82H)
                            "data_type": data_type,  # 'generation' or 'prices'
                            "psr_type": psr_type,
                            "start_time": start_time,
                            "resolution": resolution,
                            "position": int(pos),
                            "value": float(val_el.text),
                        }
                    )
        return records
    except Exception as e:
        print(f"   ⚠️ Error processing {xml_path.name}: {e}")
        return []


# ======================================================
# 3. MAIN EXECUTION
# ======================================================


def main():
    # Capture target date from CLI arguments (Airflow support) or use fallback
    if len(sys.argv) > 1:
        target_date_raw = sys.argv[1]
        # Normalize date format for path navigation (YYYY-MM-DD to YYYY/MM/DD)
        target_date = target_date_raw.replace("-", "/")
    else:
        # Fallback date for manual testing
        target_date = "2026/02/01"

    print(f"🧩 Starting Parsing for date: {target_date}")

    for dtype in DATA_TYPES:
        # Navigate to the specific partition folder
        day_dir = RAW_BASE_DIR / dtype / target_date

        if not day_dir.exists():
            print(
                f"⚠️ No raw data found for {dtype} on {target_date} at: {day_dir.absolute()}"
            )
            continue

        all_day_records = []
        # Process every XML file found for the specific day and type
        for xml_file in day_dir.glob("*.xml"):
            print(f"📄 Parsing {xml_file.name}")
            all_day_records.extend(parse_xml_to_records(xml_file, dtype))

        # Save the aggregated results into a single CSV per data type/day
        if all_day_records:
            df = pd.DataFrame(all_day_records)
            output_dir = PROCESSED_BASE_DIR / dtype / target_date
            output_dir.mkdir(parents=True, exist_ok=True)

            output_path = output_dir / f"parsed_{dtype}.csv"
            df.to_csv(output_path, index=False)
            print(f"✅ Saved {len(df)} rows to {output_path.absolute()}")


if __name__ == "__main__":
    main()
