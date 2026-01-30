"""
Enrich parsed ENTSO-E generation data with metadata (bidding zone and country).

This script performs a production-style ETL step:
1. Loads the parsed generation CSV produced by parse_generation_xml.py
2. Loads a reference table mapping bidding zones to countries
3. Enriches the dataset by filling missing bidding_zone and country fields
4. Resolves duplicated columns after merge
5. Writes a new enriched CSV to disk

Author: Jean-François Bourgeois
"""

from pathlib import Path
import pandas as pd

# ---------------------------------------------------------------------------
# Paths (relative to project root)
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]

PARSED_DATA_PATH = PROJECT_ROOT / "data" / "processed" / "entsoe_generation_parsed.csv"
REFERENCE_PATH = PROJECT_ROOT / "data" / "reference" / "bidding_zones.csv"
OUTPUT_PATH = PROJECT_ROOT / "data" / "processed" / "entsoe_generation_enriched.csv"

# ---------------------------------------------------------------------------
# Load functions
# ---------------------------------------------------------------------------

def load_parsed_data(filepath: Path) -> pd.DataFrame:
    """Load parsed ENTSO-E generation data."""
    if not filepath.exists():
        raise FileNotFoundError(f"Parsed data file not found: {filepath}")

    return pd.read_csv(filepath, parse_dates=["start_time"])


def load_reference_data(filepath: Path) -> pd.DataFrame:
    """Load bidding zone reference data."""
    if not filepath.exists():
        raise FileNotFoundError(f"Reference file not found: {filepath}")

    return pd.read_csv(filepath)


# ---------------------------------------------------------------------------
# Enrichment logic
# ---------------------------------------------------------------------------

def enrich_with_bidding_zone_metadata(
    generation_df: pd.DataFrame, reference_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Enrich generation data with bidding zone and country information.

    The ENTSO-E API does not explicitly provide country names in the XML.
    Country is derived from the bidding zone using a reference table.
    """

    # If bidding_zone is missing, assume the dataset corresponds
    # to a single zone defined in the reference table
    if generation_df["bidding_zone"].isna().all():
        if len(reference_df) != 1:
            raise ValueError(
                "Bidding zone is missing in data and reference contains multiple zones."
            )

        default_zone = reference_df.loc[0, "bidding_zone"]
        generation_df["bidding_zone"] = default_zone

    # Merge reference metadata (bidding_zone -> country)
    enriched_df = generation_df.merge(
        reference_df,
        on="bidding_zone",
        how="left",
        validate="many_to_one",  # ensure each bidding zone maps to a single country
    )

    # -------------------------
    # Resolve duplicated country columns after merge
    # -------------------------
    # Pandas creates `country_x` (from generation_df) and `country_y` (from reference_df)
    if "country_x" in enriched_df.columns:
        # Keep country_x as the final 'country' column
        enriched_df["country"] = enriched_df["country_x"]

        # Drop both duplicated columns if they exist
        for col in ["country_x", "country_y"]:
            if col in enriched_df.columns:
                enriched_df.drop(columns=col, inplace=True)

    return enriched_df


# ---------------------------------------------------------------------------
# Save function
# ---------------------------------------------------------------------------

def save_enriched_data(df: pd.DataFrame, filepath: Path) -> None:
    """Save enriched dataset to disk."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(filepath, index=False)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def main() -> None:
    print("🧩 Enriching ENTSO-E generation data with metadata...")

    # Load parsed generation data
    generation_df = load_parsed_data(PARSED_DATA_PATH)

    # Load reference table for bidding zones -> countries
    reference_df = load_reference_data(REFERENCE_PATH)

    # Enrich generation data
    enriched_df = enrich_with_bidding_zone_metadata(
        generation_df=generation_df,
        reference_df=reference_df,
    )

    # Save enriched dataset
    save_enriched_data(enriched_df, OUTPUT_PATH)

    print(f"✅ Enriched data saved to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
