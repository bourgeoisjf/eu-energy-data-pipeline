import xml.etree.ElementTree as ET
import pandas as pd
from pathlib import Path
import sys

# ======================================================
# Paths Configuration
# ======================================================
PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_BASE_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_BASE_DIR = PROJECT_ROOT / "data" / "processed"

DATA_TYPES = ["generation", "prices"]

# ======================================================
# Parsing Logic
# ======================================================


def parse_xml_to_records(xml_path: Path, data_type: str) -> list[dict]:
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
        country_code = xml_path.stem.split("_")[-1]
        records = []

        for timeseries in root.findall(".//{*}TimeSeries"):
            bz_el = timeseries.find(".//{*}in_Domain.mRID") or timeseries.find(
                ".//{*}out_Domain.mRID"
            )
            bidding_zone = bz_el.text if bz_el is not None else "Unknown"

            psr_el = timeseries.find(".//{*}psrType")
            psr_type = psr_el.text if psr_el is not None else "N/A"

            period_el = timeseries.find(".//{*}Period")
            if period_el is None:
                continue

            start_time = period_el.find(".//{*}start").text
            resolution = period_el.find(".//{*}resolution").text

            for point in period_el.findall(".//{*}Point"):
                pos = point.find("{*}position").text
                val_el = (
                    point.find("{*}price.amount")
                    if data_type == "prices"
                    else point.find("{*}quantity")
                )

                if val_el is not None:
                    records.append(
                        {
                            "country": country_code,
                            "bidding_zone": bidding_zone,
                            "type": data_type,
                            "psr_type": psr_type,
                            "start_time": start_time,
                            "resolution": resolution,
                            "position": int(pos),
                            "value": float(val_el.text),
                        }
                    )
        return records
    except Exception as e:
        print(f"   ⚠️ Erro ao processar {xml_path.name}: {e}")
        return []


# ======================================================
# Main Execution
# ======================================================


def main():
    # Pega a data via argumento (ex: 2026-02-01) ou usa fallback
    if len(sys.argv) > 1:
        target_date_raw = sys.argv[1]
        target_date = target_date_raw.replace("-", "/")
    else:
        target_date = "2026/02/01"

    print(f"🧩 Starting Parsing for date: {target_date}")

    for dtype in DATA_TYPES:
        day_dir = RAW_BASE_DIR / dtype / target_date

        if not day_dir.exists():
            print(
                f"⚠️ No raw data found for {dtype} on {target_date} em: {day_dir.absolute()}"
            )
            continue

        all_day_records = []
        for xml_file in day_dir.glob("*.xml"):
            print(f"📄 Parsing {xml_file.name}")
            all_day_records.extend(parse_xml_to_records(xml_file, dtype))

        if all_day_records:
            df = pd.DataFrame(all_day_records)
            output_dir = PROCESSED_BASE_DIR / dtype / target_date
            output_dir.mkdir(parents=True, exist_ok=True)

            output_path = output_dir / f"parsed_{dtype}.csv"
            df.to_csv(output_path, index=False)
            print(f"✅ Saved {len(df)} rows to {output_path.absolute()}")


if __name__ == "__main__":
    main()
