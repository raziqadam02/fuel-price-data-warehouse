# Data Warehouse Architecture

## Overview

This project implements an end-to-end data warehouse pipeline for fuel price analytics. It integrates multiple data sources, processes them through an ETL pipeline, and stores them in a Snowflake data warehouse for reporting and visualization in Power BI.

---

## High-Level Architecture

```
Parquet / API Sources
        ↓
Python ETL Pipeline
        ↓
Snowflake (RAW Layer)
        ↓
Transformation (SQL / Python)
        ↓
Snowflake (ANALYTICS Layer - Star Schema)
        ↓
Power BI Dashboard
```

---

## Components

### 1. Data Sources

* Fuel Price (Parquet file)
* Oil Price (API)
* Currency Exchange Rate (API)

The fuel API was replaced with a Parquet file due to limited historical data availability.

---

### 2. Ingestion Layer (`/ingestion`)

Responsible for extracting and loading raw data into Snowflake.

Key features:

* Reads Parquet files using pandas
* Fetches API data using requests
* Cleans and standardizes column names
* Inserts data into RAW tables

Main files:

* `fuel_api_ingest.py`
* `oil_api_ingest.py`
* `currency_api_ingest.py`
* `snowflake_connection.py`

---

### 3. ETL Layer (`/etl`)

Handles transformation from RAW to ANALYTICS schema.

Key processes:

* Data normalization
* Unpivoting fuel price columns (RON95, RON97, Diesel)
* Mapping to dimension tables
* Loading fact tables

Main files:

* `fuel_transform.py`
* `oil_transform.py`
* `currency_transform.py`
* `run_pipeline.py`

---

### 4. Data Warehouse (Snowflake)

#### Database

* `FUEL_DB`

#### Schemas

* `RAW`: Stores ingested raw data
* `ANALYTICS`: Stores transformed data

---

### 5. Data Model (Star Schema)

#### Dimension Tables

* `dim_date`
* `dim_fuel_type`
* `dim_currency`

#### Fact Tables

* `fact_fuel_price`
* `fact_oil_price`
* `fact_currency_rate`

---

### 6. Transformation Logic

* Fuel data is unpivoted into row format
* Oil data is mapped directly to fact table
* Currency data is linked to dimension keys
* Date relationships are maintained via `dim_date`

---

### 7. Automation

The ETL pipeline is executed via:

```
python -m etl.run_pipeline
```

Automation is handled using Windows Task Scheduler.

---

### 8. Visualization (Power BI)

The dashboard includes:

* KPI cards (latest fuel price, oil price, exchange rate)
* Line chart (fuel price trends)
* Column chart (fuel comparison)
* Table (latest values with trend indicators)
* Slicers (year, month, fuel type)

---

## Design Decisions

### Use of Parquet Instead of API

* API provided limited historical data
* Parquet enables full dataset ingestion
* Improves reliability and performance

### Star Schema

* Optimized for analytical queries
* Simplifies Power BI relationships
* Improves query performance

### Separation of RAW and ANALYTICS Layers

* RAW preserves original data
* ANALYTICS provides cleaned, structured data
* Enables easier debugging and reprocessing

---

## Future Enhancements

* Real-time ingestion using Snowflake Streams and Tasks
* Data quality validation layer
* Forecasting and predictive analytics
* Deployment to Power BI Service with scheduled refresh
