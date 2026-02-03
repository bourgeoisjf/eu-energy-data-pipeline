"""
Unified Load Script for ENTSO-E Data.
Modified to accept a target date from Airflow via command line.
"""

import os
import sys
import psycopg2
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from psycopg2.extras import execute_batch

# 1. SETUP PATHS
PROJECT_ROOT = Path(__file__).resolve().parents[1]
BASE_DATA_PATH = PROJECT_ROOT / "data" / "processed"

# Load environment variables
load_dotenv(PROJECT_ROOT / ".env")

# 2. DB CONNECTION CONFIG
IN_DOCKER = os.path.exists("/.dockerenv")
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "db" if IN_DOCKER else "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

# 3. TABLE DESIGNS (SCHEMAS)
TABLE_SCHEMAS = {
    "generation": {
        "table_name": "energy_generation",
        "create_sql": """
            CREATE TABLE IF NOT EXISTS energy_generation (
                id SERIAL PRIMARY KEY,
                country CHAR(2),
                country_name VARCHAR(50),
                type VARCHAR(20),
                bidding_zone VARCHAR(50),
                psr_type VARCHAR(10),
                generation_type VARCHAR(100),
                start_time TIMESTAMPTZ,
                resolution VARCHAR(10),
                position INT,
                quantity_mw NUMERIC,
                ingested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(country, psr_type, start_time, position)
            );
        """,
        "insert_sql": """
            INSERT INTO energy_generation 
            (country, country_name, type, bidding_zone, psr_type, generation_type, start_time, resolution, position, quantity_mw)
            VALUES (%(country)s, %(country_name)s, %(type)s, %(bidding_zone)s, %(psr_type)s, %(generation_type)s, %(start_time)s, %(resolution)s, %(position)s, %(quantity_mw)s)
            ON CONFLICT (country, psr_type, start_time, position) DO NOTHING;
        """,
    },
    "prices": {
        "table_name": "energy_prices",
        "create_sql": """
            CREATE TABLE IF NOT EXISTS energy_prices (
                id SERIAL PRIMARY KEY,
                country CHAR(2),
                country_name VARCHAR(50),
                type VARCHAR(20),
                bidding_zone VARCHAR(50),
                start_time TIMESTAMPTZ,
                resolution VARCHAR(10),
                position INT,
                price_eur NUMERIC,
                ingested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(country, start_time, position)
            );
        """,
        "insert_sql": """
            INSERT INTO energy_prices 
            (country, country_name, type, bidding_zone, start_time, resolution, position, price_eur)
            VALUES (%(country)s, %(country_name)s, %(type)s, %(bidding_zone)s, %(start_time)s, %(resolution)s, %(position)s, %(price_eur)s)
            ON CONFLICT (country, start_time, position) DO NOTHING;
        """,
    },
}


# 4. DATA LOADING ENGINE
def load_csv_to_postgres(conn, file_path, category):
    """Loads a single CSV file into the database."""
    print(f"📖 Reading: {file_path.absolute()}")

    df = pd.read_csv(file_path, parse_dates=["start_time"])
    schema = TABLE_SCHEMAS[category]

    with conn.cursor() as cur:
        cur.execute(schema["create_sql"])
        records = df.to_dict(orient="records")
        execute_batch(cur, schema["insert_sql"], records, page_size=1000)
        conn.commit()

    print(f"✅ Loaded {len(df)} rows into '{schema['table_name']}'.")


# 5. MAIN LOGIC
def main():
    # Dynamic Date Handling (same logic as parse and enrich scripts)
    if len(sys.argv) > 1:
        target_date_raw = sys.argv[1]
        target_date = target_date_raw.replace("-", "/")
    else:
        # Default to a specific date for manual testing
        target_date = "2026/02/01"

    print(f"🚀 Starting ENTSO-E Data Loader for date: {target_date}")

    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            for category in ["generation", "prices"]:
                # Targeted file path based on date
                csv_file = (
                    BASE_DATA_PATH / category / target_date / f"enriched_{category}.csv"
                )

                if csv_file.exists():
                    load_csv_to_postgres(conn, csv_file, category)
                else:
                    print(f"ℹ️ File not found for {category}: {csv_file.absolute()}")

        print("\n✨ Database update process finished.")

    except Exception as e:
        print(f"❌ Database Error: {e}")


if __name__ == "__main__":
    main()
