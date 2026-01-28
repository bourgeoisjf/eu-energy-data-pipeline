"""
validate_generation_data.py

This script performs sanity checks on the parsed ENTSO-E generation CSV.
It ensures data quality before downstream analysis or storage.
"""

import os
import csv
from collections import Counter


# ============================================================
# 1. CONSTANTS
# ============================================================

PROCESSED_FILE = "data/processed/entsoe_generation_parsed.csv"

REQUIRED_COLUMNS = {
    "psr_type",
    "generation_type",
    "bidding_zone",
    "country",
    "start_time",
    "position",
    "quantity_mw",
}


# ============================================================
# 2. VALIDATION FUNCTIONS
# ============================================================


def file_exists(path: str) -> None:
    if not os.path.exists(path):
        raise FileNotFoundError(f"❌ File not found: {path}")


def file_not_empty(path: str) -> None:
    if os.path.getsize(path) == 0:
        raise ValueError("❌ CSV file is empty")


def validate_columns(header: list[str]) -> None:
    missing = REQUIRED_COLUMNS - set(header)
    if missing:
        raise ValueError(f"❌ Missing required columns: {missing}")


def validate_rows(rows: list[dict]) -> None:
    if not rows:
        raise ValueError("❌ CSV contains no data rows")


def validate_quantities(rows: list[dict]) -> None:
    negatives = [row for row in rows if float(row["quantity_mw"]) < 0]
    if negatives:
        raise ValueError(f"❌ Found {len(negatives)} negative generation values")


def validate_missing_fields(rows: list[dict]) -> None:
    critical_fields = ["psr_type", "start_time", "quantity_mw"]

    for field in critical_fields:
        missing = [r for r in rows if not r[field]]
        if missing:
            raise ValueError(f"❌ Found {len(missing)} rows with missing '{field}'")


def report_optional_gaps(rows: list[dict]) -> None:
    missing_country = sum(1 for r in rows if not r["country"])
    missing_zone = sum(1 for r in rows if not r["bidding_zone"])

    print("ℹ️ Optional field report:")
    print(f"   • Missing country: {missing_country}")
    print(f"   • Missing bidding zone: {missing_zone}")


def report_psr_distribution(rows: list[dict]) -> None:
    counter = Counter(r["psr_type"] for r in rows)
    print("📊 PSR type distribution:")
    for psr, count in counter.items():
        print(f"   • {psr}: {count} rows")


# ============================================================
# 3. MAIN
# ============================================================


def main() -> None:
    print("🔍 Validating parsed ENTSO-E generation data...")

    file_exists(PROCESSED_FILE)
    file_not_empty(PROCESSED_FILE)

    with open(PROCESSED_FILE, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    validate_columns(reader.fieldnames)
    validate_rows(rows)
    validate_missing_fields(rows)
    validate_quantities(rows)

    report_optional_gaps(rows)
    report_psr_distribution(rows)

    print("✅ Data validation completed successfully")


# ============================================================
# 4. ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()
