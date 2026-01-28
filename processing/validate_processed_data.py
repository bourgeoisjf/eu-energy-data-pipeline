"""
Script: validate_processed_data.py
Author: Jean-François Bourgeois

Purpose:
    Validate processed electricity generation CSV data.

Input:
    data/processed/generation_fr_20260127_20260128.csv

Checks:
    1. Required columns exist
    2. No negative values for generation or consumption
    3. No missing timestamps
    4. MTU start < MTU end
"""

import os
import pandas as pd

# =========================
# Configuration
# =========================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROCESSED_FILE_PATH = os.path.join(
    BASE_DIR,
    "data",
    "processed",
    "generation_fr_20260127_20260128.csv"
)

REQUIRED_COLUMNS = [
    "time_interval_utc",
    "area",
    "unit_name",
    "unit_code",
    "type",
    "instance_code",
    "mtu_utc",
    "generation_mw",
    "consumption_mw",
    "mtu_start",
    "mtu_end"
]

# =========================
# Validation functions
# =========================

def load_data(filepath):
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")
    return pd.read_csv(filepath)


def check_columns(df):
    missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    print("✅ All required columns are present.")


def check_negative_values(df):
    for col in ["generation_mw", "consumption_mw"]:
        if (df[col] < 0).any():
            raise ValueError(f"Negative values found in {col}")
    print("✅ No negative values in generation_mw or consumption_mw.")


def check_timestamps(df):
    if df["mtu_start"].isnull().any() or df["mtu_end"].isnull().any():
        raise ValueError("Null timestamps found in mtu_start or mtu_end")
    if (df["mtu_start"] >= df["mtu_end"]).any():
        raise ValueError("Found mtu_start >= mtu_end")
    print("✅ Timestamps are valid.")


# =========================
# Main
# =========================

def main():
    df = load_data(PROCESSED_FILE_PATH)
    check_columns(df)
    check_negative_values(df)
    check_timestamps(df)
    print("🎯 Validation completed successfully!")


if __name__ == "__main__":
    main()
    