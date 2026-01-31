"""
Final enrichment of parsed ENTSO-E generation data.

This script:
- Keeps 'country' (code) and adds 'country_name'
- Converts 'psr_type' to human-readable 'generation_type'
- Handles missing references by filling with 'Unknown'
- Outputs a CSV ready for database ingestion
"""

from pathlib import Path
import pandas as pd

# -----------------------------
# Paths
# -----------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]

PARSED_CSV = PROJECT_ROOT / "data" / "processed" / "entsoe_generation_parsed.csv"
COUNTRIES_CSV = PROJECT_ROOT / "data" / "reference" / "countries.csv"
PSR_TYPES_CSV = PROJECT_ROOT / "data" / "reference" / "psr_types.csv"
OUTPUT_CSV = PROJECT_ROOT / "data" / "processed" / "generation_dataset_final.csv"


# -----------------------------
# Load functions
# -----------------------------
def load_csv(path: Path, parse_dates=None) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    return pd.read_csv(path, parse_dates=parse_dates)


# -----------------------------
# Enrichment logic
# -----------------------------
def enrich_generation_data(df: pd.DataFrame, countries_ref: pd.DataFrame, psr_ref: pd.DataFrame) -> pd.DataFrame:
    """
    Merge generation data with country names and PSR (production type) names.
    Fills missing references with 'Unknown'.
    """

    # -----------------------------
    # Merge country names
    # -----------------------------
    enriched_df = df.merge(
        countries_ref[["country", "country_name", "bidding_zone"]],
        on=["country", "bidding_zone"],
        how="left",
        validate="many_to_one"
    )

    missing_countries = enriched_df["country_name"].isna().sum()
    if missing_countries > 0:
        print(f"⚠️ {missing_countries} rows have no matching country_name. Filling with 'Unknown'.")
        enriched_df["country_name"] = enriched_df["country_name"].fillna("Unknown")

    # -----------------------------
    # Merge generation type (PSR)
    # -----------------------------
    enriched_df = enriched_df.merge(
        psr_ref,
        on="psr_type",
        how="left",
        validate="many_to_one"
    )

    missing_psr = enriched_df["generation_type"].isna().sum()
    if missing_psr > 0:
        print(f"⚠️ {missing_psr} rows have no matching generation_type. Filling with 'Unknown'.")
        enriched_df["generation_type"] = enriched_df["generation_type"].fillna("Unknown")

    # -----------------------------
    # Final column order
    # -----------------------------
    columns_order = [
        "country",
        "country_name",
        "bidding_zone",
        "psr_type",
        "generation_type",
        "start_time",
        "position",
        "quantity_mw"
    ]
    enriched_df = enriched_df[columns_order]

    return enriched_df


# -----------------------------
# Save function
# -----------------------------
def save_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


# -----------------------------
# Main pipeline
# -----------------------------
def main():
    print("🧩 Enriching ENTSO-E generation data for database ingestion...")

    df = load_csv(PARSED_CSV, parse_dates=["start_time"])
    countries_ref = load_csv(COUNTRIES_CSV)
    psr_ref = load_csv(PSR_TYPES_CSV)

    enriched_df = enrich_generation_data(df, countries_ref, psr_ref)
    save_csv(enriched_df, OUTPUT_CSV)

    print(f"✅ Final enriched dataset saved to: {OUTPUT_CSV}")
    print(f"📊 Total rows: {len(enriched_df)}")


if __name__ == "__main__":
    main()
