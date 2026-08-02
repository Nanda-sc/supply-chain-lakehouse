# Supply Chain Data Lakehouse

A batch data lakehouse pipeline built with PySpark, implementing the medallion architecture (bronze → silver → gold) on the [DataCo Smart Supply Chain dataset](https://www.kaggle.com/datasets/shashwatwork/dataco-smart-supply-chain-for-big-data-analysis). Includes a DuckDB query layer, a Streamlit dashboard, and an Airflow DAG for orchestration.

## Architecture

```
data/raw/          ← Landing zone (source CSV)
bronze/            ← Raw ingestion layer
silver/            ← Cleaned and standardized layer
gold/              ← Business aggregation layer
src/               ← Pipeline jobs (bronze, silver, gold, query)
dashboard/         ← Streamlit app
airflow/           ← Astro CLI project + DAG
tests/             ← Unit tests
config/            ← Path and environment config
```

### Medallion Layers

| Layer | Purpose |
|---|---|
| **Bronze** | Ingest raw CSV as-is, add metadata columns, partition by order date |
| **Silver** | Drop PII, rename columns to snake_case, parse dates, apply quality checks |
| **Gold** | Pre-aggregated business tables for analytics and dashboards |

### Gold Tables

| Table | Business Question |
|---|---|
| `delivery_performance` | Late delivery rates and shipping delays by market and region |
| `supplier_performance` | Sales, profit, and discount metrics by category and department |
| `shipping_analysis` | Actual vs scheduled shipping days by shipping mode |

### Dashboard

`streamlit run dashboard/app.py` → `http://localhost:8501`, with four sections: Overview KPIs, Delivery Performance, Supplier Performance, and Shipping Analysis.

### Orchestration

An Airflow DAG (`supply_chain_pipeline`, `@daily`) runs `bronze_ingest → silver_transform → [delivery_performance, supplier_performance, shipping_analysis]` in parallel. Managed via Astro CLI (requires Docker Desktop).

## Dataset

**DataCo Smart Supply Chain** — 180,519 order line items covering global e-commerce supply chain operations from 2015–2018 across markets in Africa, Europe, LATAM, Pacific Asia, and USCA.

Download from Kaggle and place `DataCoSupplyChainDataset.csv` in `data/raw/`.

## Setup

**Requirements:** Python 3.x, Java 8+ (required by Spark)

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Running the Pipeline

Run each layer in order:

```bash
# Bronze — ingest raw CSV
python src/bronze/ingest_supply_chain.py

# Silver — clean and standardize
python src/silver/transform_supply_chain.py

# Gold — business aggregations
python src/gold/delivery_performance.py
python src/gold/supplier_performance.py
python src/gold/shipping_analysis.py
```

## Running the Dashboard

```bash
streamlit run dashboard/app.py
```

## Running the Airflow DAG

Requires Docker Desktop running.

```bash
cd airflow && astro dev start
cd airflow && astro dev stop
```

## Running Tests

```bash
python -m pytest tests/ -v
```

38 unit tests covering bronze ingestion, silver transformation, and gold aggregation logic. Tests use in-memory DataFrames — no file I/O required.

## AWS Migration

Designed to be AWS-ready. To run on AWS Glue + S3, update `config/settings.py`:

```python
BRONZE_PATH = "s3://your-bucket/bronze/supply_chain"
SILVER_PATH = "s3://your-bucket/silver/supply_chain"
```

No other code changes required. Each pipeline script maps directly to a Glue job.

## Key Findings

- Late delivery rates are **50–58% across all global regions** — a systemic issue not isolated to any single market
- Western Europe is the largest market with 10,010 orders
- Average shipping delay is 0.4–0.65 days across all regions
