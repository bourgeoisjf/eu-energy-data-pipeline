from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# ======================================================
# CONFIGURATION
# ======================================================
# Absolute path to the virtual environment python executable
PYTHON_BIN = "/Volumes/Seagate5T/CienciaDeDadosBACKUP/__MyProjectsDataAnalyse/eu-energy-data-pipeline/airflow_venv/bin/python"

# Absolute path to the project root directory on the Seagate drive
PROJECT_DIR = "/Volumes/Seagate5T/CienciaDeDadosBACKUP/__MyProjectsDataAnalyse/eu-energy-data-pipeline"

# Default arguments for all tasks
default_args = {
    "owner": "data_energy",
    "depends_on_past": True,  # Ensures day 2 only runs if day 1 succeeded
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ======================================================
# DAG DEFINITION
# ======================================================
with DAG(
    "entsoe_daily_pipeline",
    default_args=default_args,
    description="ENSTSO-E Energy Data Pipeline with Backfill",
    schedule_interval="0 3 * * *",  # Runs at 03:00 AM daily
    start_date=datetime(2026, 2, 1),  # Historical start date
    catchup=True,  # Enables processing missing dates
    max_active_runs=1,  # CRITICAL: Prevents hitting API rate limits
    tags=["energy", "entsoe"],
) as dag:
    # 1. EXTRACTION: Download XML files from ENTSO-E API
    # The dynamic date is passed via {{ ds }}
    fetch_data = BashOperator(
        task_id="download_xml",
        bash_command=f"{PYTHON_BIN} {PROJECT_DIR}/ingestion/fetch_entsoe_data.py {{{{ ds }}}}",
    )

    # 2. PARSING: Convert raw XML files to structured CSV
    parse_task = BashOperator(
        task_id="parse_xml_to_csv",
        bash_command=f"{PYTHON_BIN} {PROJECT_DIR}/processing/parse_generation_xml.py {{{{ ds }}}}",
    )

    # 3. ENRICHMENT: Map country codes, PSR types and calculate final metrics
    enrich_task = BashOperator(
        task_id="enrich_data",
        bash_command=f"{PYTHON_BIN} {PROJECT_DIR}/processing/enrich_generation_data.py {{{{ ds }}}}",
    )

    # 4. LOADING: Upload the final enriched data to PostgreSQL
    load_task = BashOperator(
        task_id="load_to_postgres",
        bash_command=f"{PYTHON_BIN} {PROJECT_DIR}/processing/load_generation_to_postgres.py {{{{ ds }}}}",
    )

    # ======================================================
    # DATA PIPELINE FLOW
    # ======================================================
    # download -> parse -> enrich -> load
    fetch_data >> parse_task >> enrich_task >> load_task
