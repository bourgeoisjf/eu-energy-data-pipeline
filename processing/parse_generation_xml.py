"""
parse_generation_xml.py

This script parses raw ENTSO-E generation XML files and converts them
into a structured CSV file for further analysis.

Expected input:
- XML files in data/raw/

Output:
- CSV file in data/processed/
"""

import os
import csv
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone


# ============================================================
# 1. CONSTANTS & MAPPINGS
# ============================================================

RAW_DATA_DIR = "data/raw"
PROCESSED_DATA_DIR = "data/processed"

# PSR (Power System Resource) type mapping
PSR_TYPE_MAPPING = {
    "B01": "Biomass",
    "B02": "Fossil Brown coal/Lignite",
    "B03": "Fossil Coal-derived gas",
    "B04": "Fossil Gas",
    "B05": "Fossil Hard coal",
    "B06": "Fossil Oil",
    "B07": "Fossil Oil shale",
    "B08": "Fossil Peat",
    "B09": "Geothermal",
    "B10": "Hydro Pumped Storage",
    "B11": "Hydro Run-of-river and poundage",
    "B12": "Hydro Water Reservoir",
    "B13": "Marine",
    "B14": "Nuclear",
    "B15": "Other renewable",
    "B16": "Solar",
    "B17": "Waste",
    "B18": "Wind Offshore",
    "B19": "Wind Onshore",
    "B20": "Other",
}

# Mapping bidding zones to countries (simplified, expandable)
BIDDING_ZONE_TO_COUNTRY = {
    "10YFR-RTE------C": "France",
    "10YDE-VE-------2": "Germany",
    "10YES-REE------0": "Spain",
    "10YIT-GRTN-----B": "Italy",
}


# ============================================================
# 2. HELPER FUNCTIONS
# ============================================================


def ensure_directory_exists(path: str) -> None:
    """Create directory if it does not exist."""
    if not os.path.exists(path):
        os.makedirs(path)


def parse_iso_datetime(value: str) -> datetime:
    """Parse ISO datetime string to timezone-aware datetime."""
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


# ============================================================
# 3. CORE XML PARSING LOGIC
# ============================================================


def parse_generation_xml(xml_file_path: str) -> list[dict]:
    """
    Parse a single ENTSO-E generation XML file.

    Returns:
        List of dictionaries (rows)
    """

    tree = ET.parse(xml_file_path)
    root = tree.getroot()

    # Extract XML namespace
    namespace = root.tag.split("}")[0].strip("{")
    ns = {"ns": namespace}

    rows = []

    # Iterate over all TimeSeries elements
    for timeseries in root.findall("ns:TimeSeries", ns):
        psr_type = timeseries.find(".//ns:MktPSRType/ns:psrType", ns)
        psr_code = psr_type.text if psr_type is not None else None
        generation_type = PSR_TYPE_MAPPING.get(psr_code, "Unknown")

        bidding_zone_el = timeseries.find("ns:inBiddingZone_Domain.mRID", ns)
        bidding_zone = bidding_zone_el.text if bidding_zone_el is not None else None
        country = BIDDING_ZONE_TO_COUNTRY.get(bidding_zone, None)

        period = timeseries.find("ns:Period", ns)
        start_time_el = period.find("ns:timeInterval/ns:start", ns)
        start_time = parse_iso_datetime(start_time_el.text)

        resolution_el = period.find("ns:resolution", ns)
        resolution = resolution_el.text if resolution_el is not None else "PT60M"

        # ENTSO-E generation is usually PT15M or PT60M
        step_minutes = 15 if resolution == "PT15M" else 60

        for point in period.findall("ns:Point", ns):
            position = int(point.find("ns:position", ns).text)
            quantity = float(point.find("ns:quantity", ns).text)

            timestamp = start_time + timedelta(minutes=(position - 1) * step_minutes)

            rows.append(
                {
                    "psr_type": psr_code,
                    "generation_type": generation_type,
                    "bidding_zone": bidding_zone,
                    "country": country,
                    "start_time": timestamp.isoformat(),
                    "position": position,
                    "quantity_mw": quantity,
                }
            )

    return rows


# ============================================================
# 4. MAIN PIPELINE
# ============================================================


def main() -> None:
    print("🔄 Parsing ENTSO-E generation XML files...")

    ensure_directory_exists(PROCESSED_DATA_DIR)

    xml_files = [f for f in os.listdir(RAW_DATA_DIR) if f.endswith(".xml")]

    if not xml_files:
        raise FileNotFoundError("No XML files found in data/raw/")

    all_rows = []

    for xml_file in xml_files:
        xml_path = os.path.join(RAW_DATA_DIR, xml_file)
        print(f"📄 Processing {xml_file}")
        rows = parse_generation_xml(xml_path)
        all_rows.extend(rows)

    output_file = os.path.join(PROCESSED_DATA_DIR, "entsoe_generation_parsed.csv")

    with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = [
            "psr_type",
            "generation_type",
            "bidding_zone",
            "country",
            "start_time",
            "position",
            "quantity_mw",
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"✅ Parsed data saved to {output_file}")
    print(f"📊 Total rows: {len(all_rows)}")


# ============================================================
# 5. ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()
