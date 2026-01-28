import xml.etree.ElementTree as ET
import pandas as pd
from pathlib import Path


RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")


def parse_generation_xml(xml_path: Path) -> pd.DataFrame:
    """
    Parse ENTSO-E generation XML into a structured DataFrame.
    """

    tree = ET.parse(xml_path)
    root = tree.getroot()

    ns = {
        "ns": "urn:iec62325.351:tc57wg16:451-1:publicationdocument:7:0"
    }

    records = []

    for timeseries in root.findall("ns:TimeSeries", ns):
        area = timeseries.find(
            "ns:outBiddingZone_Domain.mRID", ns
        )
        area_code = area.text if area is not None else None

        prod_type = timeseries.find(
            "ns:MktPSRType/ns:psrType", ns
        )
        production_type = prod_type.text if prod_type is not None else None

        period = timeseries.find("ns:Period", ns)
        if period is None:
            continue

        time_interval = period.find("ns:timeInterval", ns)
        start_time = time_interval.find("ns:start", ns).text
        end_time = time_interval.find("ns:end", ns).text

        for point in period.findall("ns:Point", ns):
            quantity = point.find("ns:quantity", ns)

            if quantity is None:
                continue

            records.append({
                "start_time": start_time,
                "end_time": end_time,
                "area_code": area_code,
                "production_type": production_type,
                "quantity_mw": float(quantity.text),
                "unit": "MW"
            })

    return pd.DataFrame(records)


def main():
    xml_files = list(RAW_DIR.glob("*.xml"))

    if not xml_files:
        raise FileNotFoundError("No XML files found in data/raw/")

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    for xml_file in xml_files:
        print(f"📄 Parsing {xml_file.name}...")

        df = parse_generation_xml(xml_file)

        output_file = PROCESSED_DIR / xml_file.with_suffix(".csv").name
        df.to_csv(output_file, index=False)

        print(f"✅ Saved {output_file}")


if __name__ == "__main__":
    main()
