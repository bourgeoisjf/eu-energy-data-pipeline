"""
Final enrichment of parsed ENTSO-E data.
Now accepts date as an argument for Airflow dynamic scheduling.
"""

from pathlib import Path
import pandas as pd
import sys

# -----------------------------
# Paths Configuration
# -----------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Reference data
COUNTRIES_CSV = PROJECT_ROOT / "data" / "reference" / "countries.csv"
PSR_TYPES_CSV = PROJECT_ROOT / "data" / "reference" / "psr_types.csv"
BASE_PATH = PROJECT_ROOT / "data" / "processed"


# -----------------------------
# Enrichment Logic
# -----------------------------
def enrich_dataset(
    file_path: Path, countries_ref: pd.DataFrame, psr_ref: pd.DataFrame, category: str
):
    print(f"🔄 Processing {category}: {file_path.name}")

    df = pd.read_csv(file_path)

    # 1. Standardize Bidding Zone
    if "bidding_zone" in df.columns:
        df = df.drop(columns=["bidding_zone"])

    df["country"] = df["country"].astype(str)
    countries_ref["country"] = countries_ref["country"].astype(str)
    enriched_df = df.merge(countries_ref, on="country", how="left")
    enriched_df["country_name"] = enriched_df["country_name"].fillna("Unknown")

    # 2. CATEGORY SPECIFIC LOGIC
    if category == "prices":
        enriched_df = enriched_df.rename(columns={"value": "price_eur"})
        final_cols = [
            "country",
            "country_name",
            "type",
            "bidding_zone",
            "start_time",
            "resolution",
            "position",
            "price_eur",
        ]
    else:  # generation
        enriched_df["psr_type"] = enriched_df["psr_type"].fillna("N/A").astype(str)
        psr_ref["psr_type"] = psr_ref["psr_type"].astype(str)
        enriched_df = enriched_df.merge(psr_ref, on="psr_type", how="left")
        enriched_df["generation_type"] = enriched_df["generation_type"].fillna(
            "Unknown/Other"
        )
        enriched_df = enriched_df.rename(columns={"value": "quantity_mw"})
        final_cols = [
            "country",
            "country_name",
            "type",
            "bidding_zone",
            "psr_type",
            "generation_type",
            "start_time",
            "resolution",
            "position",
            "quantity_mw",
        ]

    # 3. Final selection and save
    enriched_df = enriched_df[final_cols]
    output_path = file_path.parent / f"enriched_{category}.csv"
    enriched_df.to_csv(output_path, index=False)

    print(f"✅ Saved enriched {category} to: {output_path.absolute()}")
    return len(enriched_df)


def main():
    # Pega a data passada pelo Airflow ou usa a fixa como fallback
    # O Airflow manda no formato YYYY-MM-DD, vamos converter para YYYY/MM/DD
    if len(sys.argv) > 1:
        target_date_raw = sys.argv[1]
        target_date = target_date_raw.replace("-", "/")
    else:
        target_date = "2026/02/01"

    print(f"🧩 Starting enrichment for date: {target_date}")

    # Load references
    countries_ref = pd.read_csv(COUNTRIES_CSV)
    psr_ref = pd.read_csv(PSR_TYPES_CSV)

    categories = ["generation", "prices"]
    total_rows = 0

    for cat in categories:
        input_file = BASE_PATH / cat / target_date / f"parsed_{cat}.csv"

        if input_file.exists():
            rows = enrich_dataset(input_file, countries_ref, psr_ref, cat)
            total_rows += rows
        else:
            print(f"⚠️ File not found: {input_file}")

    print(f"\n✨ Enrichment complete! Total rows processed: {total_rows}")


if __name__ == "__main__":
    main()
