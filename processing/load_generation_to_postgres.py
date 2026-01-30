import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
import os

# -----------------------------
# Configuration
# -----------------------------
CSV_PATH = "data/processed/generation_dataset_final.csv"

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "energy",
    "user": "postgres",
    "password": "1234"
}

# -----------------------------
# Load CSV data
# -----------------------------
def load_csv(filepath: str) -> pd.DataFrame:
    print(f"📄 Loading CSV from {filepath}")
    df = pd.read_csv(filepath, parse_dates=["start_time"])
    return df

# -----------------------------
# Insert data into PostgreSQL
# -----------------------------
def insert_data(df: pd.DataFrame):
    print("🗄️ Connecting to PostgreSQL...")
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    insert_query = """
        INSERT INTO energy_generation (
    psr_type,
    generation_type,
    bidding_zone,
    country,
    start_time,
    position,
    quantity_mw
    )

    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (country, bidding_zone, psr_type, start_time)
    DO UPDATE SET
        quantity_mw = EXCLUDED.quantity_mw,
        position = EXCLUDED.position;
        
        """

    records = df[
        [
            "country",
            "bidding_zone",
            "psr_type",
            "generation_type",
            "start_time",
            "position",
            "quantity_mw",
        ]
    ].values.tolist()

    print(f"📥 Inserting {len(records)} rows...")
    execute_batch(cursor, insert_query, records, page_size=1000)

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Data successfully loaded into PostgreSQL")

# -----------------------------
# Main
# -----------------------------
def main():
    df = load_csv(CSV_PATH)
    insert_data(df)

if __name__ == "__main__":
    main()
