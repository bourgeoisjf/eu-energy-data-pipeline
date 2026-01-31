# EU Energy Data Pipeline (ENTSO-E)

End-to-end **data engineering & analytics pipeline** built with real ENTSO-E API data, designed as a **professional portfolio project**.

The project ingests electricity generation data for multiple European countries, processes and enriches it, stores it in PostgreSQL, and makes it ready for analysis and visualization in tools like **Power BI**.

---

## 🎯 Project Goals

* Build a **realistic, production-style data pipeline**
* Work with **real energy market data** (ENTSO-E API)
* Demonstrate skills in:

  * API ingestion
  * XML parsing
  * Data cleaning & enrichment
  * Relational databases (PostgreSQL)
  * Analytics-ready data modeling
* Create a **strong data analytics / data engineering portfolio project**

---

## 🧱 Architecture Overview

```
ENTSO-E API
    ↓
Ingestion (XML)
    ↓
Parsing (structured CSV)
    ↓
Enrichment (reference data)
    ↓
Final analytics dataset
    ↓
PostgreSQL
    ↓
Power BI / Analytics
```

---

## 📁 Project Structure

```
eu-energy-data-pipeline/
│
├── ingestion/
│   └── fetch_entsoe_data.py
│
├── processing/
│   ├── parse_generation_xml.py
│   ├── enrich_generation_data.py
│   └── load_generation_to_postgres.py
│
├── data/
│   ├── raw/
│   │   └── generation/          # Raw XML files (per country)
│   │
│   ├── processed/
│   │   ├── entsoe_generation_parsed.csv
│   │   └── entsoe_generation_final.csv
│   │
│   └── reference/
│       ├── countries.csv
│       └── psr_types.csv
│
├── .env
├── requirements.txt
└── README.md
```

---

## 🌍 Countries Covered

The pipeline is **multi-country by design**.

Currently included:

* 🇫🇷 France (FR)
* 🇩🇪 Germany (DE)
* 🇪🇸 Spain (ES)
* 🇮🇹 Italy (IT)

New countries can be added easily via `countries.csv`.

---

## 🔐 Environment Variables

Create a `.env` file in the project root:

```
ENTSOE_API_KEY=your_entsoe_api_key_here
```

> The `.env` file is intentionally **not tracked by Git**.

---

## ⚙️ Pipeline Steps

### 1️⃣ Ingestion – ENTSO-E API → XML

**Script:**

```
ingestion/fetch_entsoe_data.py
```

What it does:

* Connects to the ENTSO-E API
* Downloads **actual electricity generation data**
* Fetches data for **multiple countries**
* Stores raw XML files in:

```
data/raw/generation/
```

---

### 2️⃣ Parsing – XML → Structured CSV

**Script:**

```
processing/parse_generation_xml.py
```

What it does:

* Reads all XML files from `data/raw/generation/`
* Handles ENTSO-E XML namespaces correctly
* Extracts:

  * country
  * bidding zone
  * PSR type
  * timestamp
  * position
  * generation value (MW)

Output:

```
data/processed/entsoe_generation_parsed.csv
```

---

### 3️⃣ Enrichment – Business-Friendly Dataset

**Script:**

```
processing/enrich_generation_data.py
```

What it does:

* Enriches parsed data using reference tables
* Adds:

  * Full country name (`country_name`)
  * Human-readable generation type (`generation_type`)
* Keeps both **codes and descriptive fields** (best practice)

Final output:

```
data/processed/entsoe_generation_final.csv
```

Final schema:

```
country,
country_name,
bidding_zone,
psr_type,
generation_type,
start_time,
position,
quantity_mw
```

---

### 4️⃣ Load – CSV → PostgreSQL

**Script:**

```
processing/load_generation_to_postgres.py
```

What it does:

* Loads the final dataset into PostgreSQL
* Designed for analytics & BI consumption
* Ready for Power BI dashboards

---

## 🧠 Design Decisions (Professional Rationale)

* **Multi-country pipeline** → richer analysis & stronger portfolio
* **Raw → Parsed → Final layers** → clear data lineage
* **Reference tables** → scalable & maintainable enrichment
* **Codes + labels together** → BI-friendly, no hidden joins
* **CSV as interface layer** → simple, transparent, debuggable

---

## 📊 Use Cases

* Compare energy generation mix across countries
* Analyze renewable vs non-renewable production
* Track temporal patterns in electricity generation
* Build interactive dashboards in Power BI

---

## 🚀 Next Possible Improvements

* Incremental loads (no full truncate)
* Airflow or Prefect orchestration
* Dockerization
* Star schema in PostgreSQL
* Automated data quality checks

---

## 👤 Author

**Data Analytics & Data Engineering Portfolio Project**
Built with real-world constraints and professional best practices in mind.

---

If you’re a recruiter or data professional reviewing this project:
This pipeline reflects how I approach **real data problems**, not toy examples.
