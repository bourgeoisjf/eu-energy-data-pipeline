import xml.etree.ElementTree as ET
import pandas as pd
from pathlib import Path

# Paths
RAW_DATA_DIR = Path("data/raw")
PROCESSED_DATA_DIR = Path("data/processed")
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

# ENTSO-E namespace
NS = {
    "ns": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"
}


def parse_generation_xml(xml_path: Path) -> pd.DataFrame:
    """
    Parse ENTSO-E generation XML file and return a DataFrame
    """
    tree = ET.parse(xml_path)
    root = tree.getroot()

    records = []

    # Iterate over TimeSeries
    for ts in root.findall("ns:TimeSeries", NS):
        psr_type = ts.findtext("ns:MktPSRType/ns:psrType", namespaces=NS)

        for period in ts.findall("ns:Period", NS):
            start_time = period.findtext(
                "ns:timeInterval/ns:start", namespaces=NS
            )

            for point in period.findall("ns:Point", NS):
                position = point.findtext("ns:position", namespaces=NS)
                quantity = point.findtext("ns:quantity", namespaces=NS)

                records.append({
                    "psr_type": psr_type,
                    "start_time": start_time,
                    "position": int(position),
                    "quantity_mw": float(quantity)
                })

    return pd.DataFrame(records)


def main():
    xml_files = list(RAW_DATA_DIR.glob("*.xml"))

    if not xml_files:
        raise FileNotFoundError("No XML files found in data/raw/")

    all_data = []

    for xml_file in xml_files:
        print(f"📄 Parsing {xml_file.name}")
        df = parse_generation_xml(xml_file)
        all_data.append(df)

    final_df = pd.concat(all_data, ignore_index=True)

    output_path = PROCESSED_DATA_DIR / "entsoe_generation_fr_processed.csv"
    final_df.to_csv(output_path, index=False)

    print(f"✅ CSV created with {len(final_df)} rows:")
    print(f"➡️ {output_path}")


if __name__ == "__main__":
    main()
