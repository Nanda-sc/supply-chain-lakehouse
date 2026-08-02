# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A batch data lakehouse pipeline implementing the **medallion architecture** (bronze → silver → gold) on the DataCo Supply Chain dataset (180,519 order line items, 2015–2018). Built with PySpark locally, designed to migrate to AWS Glue + S3 by changing paths in `config/settings.py`.

Demand forecasting (Moving Average + Prophet) is complete. Pipeline covers bronze → silver → gold + forecasting + dashboard + 54 tests.

## Environment Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Requires Java 8+ for Spark. Raw CSV must be placed at `data/raw/DataCoSupplyChainDataset.csv`.

## Common Commands

```bash
# Run full pipeline in order
python src/bronze/ingest_supply_chain.py
python src/silver/transform_supply_chain.py
python src/gold/delivery_performance.py
python src/gold/supplier_performance.py
python src/gold/shipping_analysis.py
python src/gold/forecast_accuracy.py    # MA + Prophet + accuracy KPIs (~10 min)

# Run dashboard
streamlit run dashboard/app.py

# Run all tests
python -m pytest tests/ -v

# Run a single test file
python -m pytest tests/test_bronze.py -v

# Run a single test
python -m pytest tests/test_bronze.py::test_add_metadata_adds_ingestion_timestamp -v

# Airflow (requires Docker Desktop running)
cd airflow && astro dev start
cd airflow && astro dev stop
cd airflow && astro dev restart
```

## Architecture

### Medallion Layers

Each layer reads from the one above it — nothing skips layers.

**Bronze** (`src/bronze/ingest_supply_chain.py`)
- Reads raw CSV, adds `ingestion_timestamp` and `source_file_name` metadata columns
- Parses `order date (DateOrders)` (format: `M/d/yyyy H:mm`) into `order_date` DateType
- Writes Parquet partitioned by `order_date` → 1,128 date partitions

**Silver** (`src/silver/transform_supply_chain.py`)
- Drops PII: `Customer Email`, `Customer Password`, `Customer Street`
- Drops junk: `Product Description`, `Product Image`
- Renames all 53 columns to snake_case (see `COLUMN_RENAMES` dict in the file)
- Parses `shipping_date` from `shipping_date_raw`
- Quality checks: dedup on `(order_id, order_item_id)`, dropna on `(order_id, customer_id, order_date)`
- Adds `silver_timestamp`

**Gold** (`src/gold/`)
- Four independent scripts, run in parallel in the Airflow DAG
- No partitioning — small aggregated tables read whole by DuckDB
- `delivery_performance` — grouped by `market, order_region`
- `supplier_performance` — grouped by `department_name, category_name`
- `shipping_analysis` — grouped by `shipping_mode, market`
- `forecast_accuracy` — weekly forecasts + accuracy KPIs (MA + Prophet)

**Forecasting** (`src/forecasting/`)
- `moving_average.py` — PySpark window functions for 4w/8w/12w MA; `get_forecast(spark)` returns full weekly DF
- `prophet_forecast.py` — Prophet via `applyInPandas` per `(market, category_name)` group; minimum 8 train weeks required
- `accuracy_metrics.py` — pure Pandas: `compute_kpis()` and `build_accuracy_summary()` for RMSE, MAE, WMAPE, Bias
- Train: 2015–2016 (before `TEST_START = "2017-01-01"`), Test: 2017
- MA_4w is the best model (WMAPE ~54%); Prophet performs poorly on this sparse dataset (WMAPE ~1982%)
- Prophet over-forecasts / predicts negatives for low-volume groups — known behaviour for this dataset

**Query layer** (`src/query/duck_db.py`)
- DuckDB reads gold Parquet files directly with SQL
- Returns pandas DataFrames consumed by Streamlit
- Functions for full tables, pre-aggregated summaries, and forecast results/accuracy

**Dashboard** (`dashboard/app.py`)
- Streamlit + Plotly, runs at `http://localhost:8501`
- Five sections: Overview KPIs, Delivery Performance, Supplier Performance, Shipping Analysis, Demand Forecasting
- Forecasting page: model accuracy table + interactive Forecast vs Actual chart with market/category selectors

### Path Configuration

All paths live in `config/settings.py`. To migrate to AWS:
```python
BRONZE_PATH = "s3://your-bucket/bronze/supply_chain"
SILVER_PATH = "s3://your-bucket/silver/supply_chain"
```
No other code changes needed.

### Airflow DAG

DAG: `supply_chain_pipeline` — scheduled `@daily`, `catchup=False`

```
bronze_ingest → silver_transform → [gold_delivery_performance,
                                    gold_supplier_performance,
                                    gold_shipping_analysis,
                                    gold_forecast_accuracy]
```

Runs via Astro CLI (Docker). Project files are copied into the container at `/usr/local/airflow/project` via the Dockerfile `COPY` directive.

## Testing Approach

Tests use small in-memory PySpark DataFrames — no file I/O, no dependency on actual CSV or Parquet files. A single shared `SparkSession` (scope=`session`) is defined in `tests/conftest.py`.

Each transformation function is tested in isolation. The `make_*_df()` helper in each test file creates the minimal DataFrame needed for that layer's tests.

Current coverage: 54 tests (6 bronze, 18 silver, 14 gold, 16 forecasting).
