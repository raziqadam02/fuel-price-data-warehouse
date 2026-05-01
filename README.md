# Fuel Data Warehouse & Analytics Dashboard

## Overview

This project implements an end-to-end data warehouse solution for analyzing fuel prices, oil prices, and currency exchange rates. It integrates data ingestion, transformation, storage, and visualization into a single pipeline using Snowflake and Power BI.

The system was designed to handle real-world data limitations by replacing unreliable API sources with Parquet-based ingestion, ensuring scalability, consistency, and improved performance.

---

## Architecture

Data flows through the following pipeline:

Parquet / API Sources
→ Python ETL Pipeline
→ Snowflake RAW Layer
→ Transformation (Star Schema)
→ ANALYTICS Layer
→ Power BI Dashboard

---

## Tech Stack

* Python (ETL pipeline)
* Snowflake (Data warehouse)
* Power BI (Visualization)
* Parquet / CSV (Data source)
* Windows Task Scheduler (Automation)

---

## Data Sources

| Source             | Status     | Notes                                 |
| ------------------ | ---------- | ------------------------------------- |
| Fuel Price API     | Deprecated | Limited to 2017 data                  |
| Fuel Price Parquet | Active     | Replaced API for full historical data |
| Oil Price API      | Active     | Working                               |
| Currency API       | Active     | Working                               |

---

## Data Warehouse Design

### Database

FUEL_DB

### Schemas

* RAW: Stores ingested data
* ANALYTICS: Stores transformed data (Star Schema)

---

## RAW Tables

* fuel_price_raw
* oil_price_raw
* currency_rate_raw

Each table includes:

* ingestion_time for tracking load history

---

## Star Schema (ANALYTICS)

### Dimension Tables

* dim_date
* dim_fuel_type
* dim_currency

### Fact Tables

* fact_fuel_price
* fact_oil_price
* fact_currency_rate

---

## Key Transformations

* Fuel data is unpivoted (RON95, RON97, Diesel → row-based format)
* Oil data is mapped directly
* Currency data is normalized with dimension mapping
* Date relationships handled via dim_date

---

## ETL Pipeline

Main execution:

```
python -m etl.run_pipeline
```

Pipeline includes:

* Data ingestion (Parquet / API)
* Data transformation
* Load into Snowflake
* Logging via etl_control table

---

## Data Source Upgrade

The original fuel API was limited to historical data (2017 only).
To resolve this, the ingestion layer was redesigned to support Parquet files.

Changes made:

* Replaced API call with pandas.read_parquet
* Maintained existing Snowflake loading logic
* No changes required in downstream transformations

This approach improves:

* Data availability
* Pipeline stability
* Performance

---

## Automation

The pipeline is automated using Windows Task Scheduler.

* Scheduled execution of ETL pipeline
* No manual intervention required
* Ensures data is consistently updated

---

## Power BI Dashboard

### Key Features

* KPI Cards

  * Latest RON95 price
  * Average oil price
  * USD to MYR exchange rate

* Line Chart

  * Fuel price trends over time

* Column Chart

  * Fuel price comparison (latest values)

* Table

  * Latest fuel prices with trend indicators

* Slicers

  * Year
  * Month
  * Fuel Type

---

## Data Modeling Considerations

* Average aggregation is used for price-based metrics
* Latest value logic is implemented using DAX measures
* Star schema ensures efficient filtering and performance
* Relationships:

  * dim_date → fact tables
  * dim_fuel_type → fact_fuel_price
  * dim_currency → fact_currency_rate

---

## Key Insights

* Fuel prices show increasing trends in recent years
* Diesel exhibits higher volatility compared to RON95
* Oil price movements influence fuel pricing patterns
* Currency fluctuations (USD/MYR) provide additional context

---

## How to Run

1. Clone the repository
2. Install dependencies

```
pip install -r requirements.txt
```

3. Ensure Snowflake connection is configured
4. Place fuelprice.parquet in the project directory
5. Run pipeline

```
python -m etl.run_pipeline
```
Note:
To use the Power BI dashboard, update the data source connection to your own Snowflake instance.
---

## Future Improvements

* Real-time data refresh using Snowflake Tasks and Streams
* Forecasting and predictive analytics
* Enhanced anomaly detection
* Deployment to Power BI Service with scheduled refresh

---

## Conclusion

This project demonstrates a complete data engineering workflow, from ingestion to visualization. It highlights practical problem-solving by adapting to data source limitations and applying industry-standard data warehouse design principles.

It is suitable for showcasing skills in data engineering, data modeling, and business intelligence.
