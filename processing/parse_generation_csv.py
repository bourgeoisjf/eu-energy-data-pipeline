"""
Script: parse_generation_csv.py
Author: Jean-François Bourgeois

Purpose:
    Parse raw electricity generation CSV data,
    clean and normalize the dataset,
    and store it in the processed layer.

Input:
    data/raw/sample_generation_fr_20260127_20260128.csv

Output:
    data/processed/generation_fr_20260127_20260128.csv
"""

import os
import pandas as pd


# =========================
# Configuration
# =========================

RAW_FILE_PATH = "data/raw/sample_generation_fr_20260127_20260128.csv"
OUTPUT_DIR = "data/processed"
OUTPUT_FILE_NAME = "generation_fr_20260127_20260128.csv"


# =========================
# Processing functions
# =========================

def load_raw_data(filepath: str) -> pd.DataFrame:
    """Load raw CSV data."""
    return pd.read_csv(filepath)


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names."""
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("(", "")
        .str.replace(")", "")
    )
    return df


def parse_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Parse datetime columns."""
    df["time_interval_utc"] = df["time_interval_utc"].astype(str)
    df["mtu_utc"] = df["mtu_utc"].astype(str)

    df["mtu_start"] = df["mtu_utc"].str.split(" - ").str[0]
    df["mtu_end"] = df["mtu_utc"].str.split(" - ").str[1]

    df["mtu_start"] = pd.to_datetime(df["mtu_start"], format="%d/%m/%Y %H:%M")
    df["mtu_end"] = pd.to_datetime(df["mtu_end"], format="%d/%m/%Y %H:%M")

    return df


def clean_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Convert numeric columns and handle missing values."""
    df["generation_mw"] = pd.to_numeric(df["generation_mw"], errors="coerce")
    df["consumption_mw"] = pd.to_numeric(df["consumption_mw"], errors="coerce")
    return df


def save_processed_data(df: pd.DataFrame):
    """Save processed data to disk."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE_NAME)
    df.to_csv(output_path, index=False)
    print(f"Processed data saved to {output_path}")


# =========================
# Main execution
# =========================

def main():
    df = load_raw_data(RAW_FILE_PATH)
    df = clean_column_names(df)
    df = parse_datetime_columns(df)
    df = clean_numeric_columns(df)
    save_processed_data(df)


if __name__ == "__main__":
    main()
