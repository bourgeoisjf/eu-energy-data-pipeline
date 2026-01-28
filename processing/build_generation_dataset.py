#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
build_generation_dataset.py

This script loads the enriched ENTSO-E generation data,
ensures all necessary columns are present, cleans and transforms
the data, and saves a final dataset ready for analysis.

Author: Jean-François Bourgeois
Date: 2026-01-27
"""

import pandas as pd
import os

# -----------------------------
# Define file paths
# -----------------------------
ENRICHED_CSV_PATH = "data/processed/entsoe_generation_enriched.csv"
FINAL_DATASET_PATH = "data/processed/generation_dataset_final.csv"


# -----------------------------
# Load enriched data
# -----------------------------
def load_enriched_data(filepath: str) -> pd.DataFrame:
    """Load the enriched CSV. Raise error if file is missing or empty."""
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Enriched CSV not found at {filepath}")
    df = pd.read_csv(filepath, parse_dates=["start_time"])
    if df.empty:
        raise ValueError("Enriched CSV is empty")
    return df


# -----------------------------
# Clean and transform data
# -----------------------------
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure all required columns exist, clean missing values,
    and sort the data for analysis.
    """
    # Columns we want in the final dataset
    required_columns = [
        "psr_type",
        "generation_type",
        "bidding_zone",
        "country",
        "start_time",
        "position",
        "quantity_mw",
    ]

    # Create missing columns with None
    for col in required_columns:
        if col not in df.columns:
            df[col] = None

    # Keep only the required columns
    df = df[required_columns]

    # Optional: drop rows where quantity_mw is missing
    df = df.dropna(subset=["quantity_mw"])

    # Sort data for consistency
    df = df.sort_values(by=["start_time", "psr_type", "position"]).reset_index(
        drop=True
    )

    return df


# -----------------------------
# Save final dataset
# -----------------------------
def save_final_dataset(df: pd.DataFrame, filepath: str):
    """Save the cleaned and transformed dataset to CSV."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    df.to_csv(filepath, index=False)
    print(f"✅ Final dataset saved to {filepath}")


# -----------------------------
# Main function
# -----------------------------
def main():
    print(f"🔍 Loading enriched data from {ENRICHED_CSV_PATH}")
    enriched_df = load_enriched_data(ENRICHED_CSV_PATH)

    print("🧹 Cleaning and transforming data...")
    analytics_df = transform_data(enriched_df)

    save_final_dataset(analytics_df, FINAL_DATASET_PATH)
    print("🚀 Dataset build completed successfully!")


# -----------------------------
# Entry point
# -----------------------------
if __name__ == "__main__":
    main()
