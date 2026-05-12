# Exploring Lombardy through Data
### Pollution, Weather, Mobility and Tourism Data from 2023
*Predictive Analytics Project — 101801*

**Acossi Giorgio Roberto — 5339095**  
DIBRIS · Computer Science · Data Analytics · Genoa, May 2026

---

## Overview

This project analyzes air quality, weather, traffic and tourism data for the Lombardy region in 2023, using official datasets from ARPA Lombardia, the Municipality of Milan and ISTAT. It is structured in two phases:

1. **ETL pipeline:** extracts, cleans and loads all sources into a PostgreSQL data warehouse.
2. **Analysis:** four Jupyter notebooks address the defined business questions using Advanced SQL, Association Rule Mining and Machine Learning.

---

## Project Structure

```
pap/
├── analysis/                                   # Jupyter notebooks for data analysis
│   ├── 1_air_quality.ipynb
│   ├── 2_weather_pollution.ipynb
│   ├── 3_traffic.ipynb
│   └── 4_human_activities.ipynb
├── datasets/                                   # CSV and GeoJSON datasets
├── docs/
│   └── docs.tex                                 # LaTeX project report
├── etl/
│   ├── main.py                                  # Pipeline orchestrator
│   ├── pipelines.py                             # ETL pipeline classes
│   ├── sources.py                               # Source classes
│   ├── tables.py                                # Table classes
│   ├── utils.py                
│   ├── logger.py
│   ├── init_db.sql                              # Schema creation script
│   └── drop_db.sql                              # Schema teardown script
├── infra/
│   └── docker-compose.yaml                      # Infrastructure definition
└── requirements.txt
```

---

## Datasets

Due to size constraints, the datasets are **not included** in this repository and must be downloaded separately. The full list of sources and download instructions can be found in `docs/docs.tex`. Once downloaded, place the files under the `datasets/` directory following the structure shown above.

---

## Prerequisites

| Tool | Version |
|------|---------|
| Python | ≥ 3.11 |
| Docker & Docker Compose | any recent version |

---

## Setup

### 1. Start the database

```bash
docker compose -f infra/docker-compose.yaml up -d
```

This starts:
- **PostgreSQL 17** on `localhost:5432` (db: `pap`, user/password: `admin`)
- **pgAdmin 4** on `http://localhost:8080` (login: `admin@admin.com` / `admin`)

### 2. Create a virtual environment and install dependencies

```bash
python -m venv venv
# Windows
venv\Scripts\activate
# Linux / macOS
source venv/bin/activate

pip install -r requirements.txt
```

### 3. Run the ETL pipeline

```bash
cd etl
python main.py
```

The pipeline is **fully idempotent**: each run drops and recreates the entire schema before loading data.

### 4. Open the analysis notebooks

```bash
jupyter lab
```

Run the notebooks in `analysis/` in order.

---

## Documentation

For detailed information on data sources, data warehouse schema, and a full description of the analyses performed (business questions, methodology and results), see `docs/docs.tex`.
