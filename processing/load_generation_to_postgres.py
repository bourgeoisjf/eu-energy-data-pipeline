"""
Load ENTSO-E generation dataset into PostgreSQL.
This script:
1. Creates the table with proper types if it doesn't exist
2. Inserts data from the CSV file
3. Ensures no duplicate rows are inserted
"""

import pandas as pd
from pathlib import Path
import psycopg2
from psycopg2.extras import execute_batch

# ------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------
CSV_PATH = Path("data/processed/generation_dataset_final.csv")

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "energy",
    "user": "postgres",
    "password": "1234",
}

TABLE_NAME = "energy_generation"


# ------------------------------------------------------------------------
# Load CSV
# ------------------------------------------------------------------------
def load_csv(filepath: Path) -> pd.DataFrame:
    if not filepath.exists():
        raise FileNotFoundError(f"CSV file not found: {filepath}")
    return pd.read_csv(filepath, parse_dates=["start_time"])


# ------------------------------------------------------------------------
# Create table if not exists
# ------------------------------------------------------------------------
def create_table(conn):
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id SERIAL PRIMARY KEY,
        country CHAR(2) NOT NULL,
        country_name VARCHAR(50),
        bidding_zone VARCHAR(20),
        psr_type VARCHAR(10),
        generation_type VARCHAR(50),
        start_time TIMESTAMPTZ,
        position INT,
        quantity_mw NUMERIC,
        UNIQUE(country, bidding_zone, psr_type, start_time, position)
    );
    """
    with conn.cursor() as cur:
        cur.execute(create_sql)
        conn.commit()
    print(f"✅ Table '{TABLE_NAME}' ensured.")


# ------------------------------------------------------------------------
# Insert data
# ------------------------------------------------------------------------
def insert_data(conn, df: pd.DataFrame):
    records = df.to_dict(orient="records")

    insert_sql = f"""
    INSERT INTO {TABLE_NAME} 
        (country, country_name, bidding_zone, psr_type, generation_type, start_time, position, quantity_mw)
    VALUES 
        (%(country)s, %(country_name)s, %(bidding_zone)s, %(psr_type)s, %(generation_type)s, %(start_time)s, %(position)s, %(quantity_mw)s)
    ON CONFLICT (country, bidding_zone, psr_type, start_time, position) DO NOTHING;
    """

    with conn.cursor() as cur:
        execute_batch(cur, insert_sql, records, page_size=1000)
        conn.commit()
    print(f"✅ Inserted {len(records)} rows (duplicates ignored).")


# ------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------
def main():
    print(f"📄 Loading CSV from {CSV_PATH}")
    df = load_csv(CSV_PATH)

    print("🗄️ Connecting to PostgreSQL...")
    with psycopg2.connect(**DB_CONFIG) as conn:
        create_table(conn)
        insert_data(conn, df)

    print("🎉 Data load completed!")


if __name__ == "__main__":
    main()
