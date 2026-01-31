"""
Parse ENTSO-E generation XML files into a structured CSV dataset.

This parser is intentionally namespace-agnostic to handle
ENTSO-E schema version changes safely.

Input:
- XML files located in data/raw/generation/

Output:
- data/processed/entsoe_generation_parsed.csv

Each XML file is expected to follow the naming convention:
generation_<COUNTRY_CODE>.xml
Example: generation_FR.xml
"""

from pathlib import Path
import xml.etree.ElementTree as ET
import pandas as pd

# =============================================================================
# Paths (relative to project root)
# =============================================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw" / "generation"
OUTPUT_PATH = PROJECT_ROOT / "data" / "processed" / "entsoe_generation_parsed.csv"


# =============================================================================
# XML Parsing Logic
# =============================================================================

def parse_generation_xml(xml_path: Path) -> list[dict]:
    """
    Parse a single ENTSO-E generation XML file.

    This function:
    - Ignores XML namespaces (robust to ENTSO-E changes)
    - Extracts generation per PSR type
    - Infers country from the filename
    """

    tree = ET.parse(xml_path)
    root = tree.getroot()

    # Infer country from filename: generation_FR.xml -> FR
    country_code = xml_path.stem.split("_")[-1]

    records = []

    # Iterate over all TimeSeries elements (namespace-agnostic)
    for timeseries in root.findall(".//{*}TimeSeries"):

        psr_type_el = timeseries.find(".//{*}psrType")
        bidding_zone_el = timeseries.find(".//{*}inBiddingZone_Domain.mRID")
        period_el = timeseries.find(".//{*}Period")

        # Skip malformed TimeSeries blocks
        if psr_type_el is None or period_el is None:
            continue

        psr_type = psr_type_el.text
        bidding_zone = bidding_zone_el.text if bidding_zone_el is not None else None

        start_time = period_el.find(".//{*}start").text

        # Iterate over all Points inside the Period
        for point in period_el.findall(".//{*}Point"):
            position = int(point.find("{*}position").text)
            quantity = float(point.find("{*}quantity").text)

            records.append(
                {
                    "country": country_code,
                    "bidding_zone": bidding_zone,
                    "psr_type": psr_type,
                    "start_time": start_time,
                    "position": position,
                    "quantity_mw": quantity,
                }
            )

    return records


# =============================================================================
# Main Pipeline
# =============================================================================

def main() -> None:
    print("🧩 Parsing ENTSO-E generation XML files...")

    if not RAW_DATA_DIR.exists():
        raise FileNotFoundError(f"Raw data directory not found: {RAW_DATA_DIR}")

    xml_files = list(RAW_DATA_DIR.glob("generation_*.xml"))

    if not xml_files:
        raise FileNotFoundError("No generation XML files found.")

    all_records = []

    for xml_file in xml_files:
        print(f"📄 Parsing {xml_file.name}")
        records = parse_generation_xml(xml_file)
        all_records.extend(records)

    df = pd.DataFrame(all_records)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)

    print(f"✅ Parsed dataset saved to: {OUTPUT_PATH}")
    print(f"📊 Total rows: {len(df)}")


if __name__ == "__main__":
    main()
