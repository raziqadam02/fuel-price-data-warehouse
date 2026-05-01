from ingestion.snowflake_connection import get_connection

from ingestion.fuel_api_ingest import run_ingestion as run_fuel_ingestion
from ingestion.oil_api_ingest import run_ingestion as run_oil_ingestion
from ingestion.currency_api_ingest import run_ingestion as run_currency_ingestion

from etl.fuel_transform import load_fuel_fact
from etl.oil_transform import load_oil_fact
from etl.currency_transform import load_currency_fact

from logs.etl_logs import log_pipeline

import logging
import os

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename="logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def run_fuel_pipeline(conn):
    logging.info("Starting fuel pipeline")

    run_fuel_ingestion(conn)
    inserted, updated = load_fuel_fact(conn)

    log_pipeline(
        conn,
        pipeline_name="fuel_pipeline",
        inserted=inserted,
        updated=updated,
        status="SUCCESS"
    )

    logging.info(f"Fuel pipeline completed | inserted={inserted}, updated={updated}")
    print("Fuel pipeline completed")


def run_oil_pipeline(conn):
    logging.info("Starting oil pipeline")

    run_oil_ingestion(conn)
    inserted, updated = load_oil_fact(conn)

    log_pipeline(
        conn,
        pipeline_name="oil_pipeline",
        inserted=inserted,
        updated=updated,
        status="SUCCESS"
    )

    logging.info(f"Oil pipeline completed | inserted={inserted}, updated={updated}")
    print("Oil pipeline completed")


def run_currency_pipeline(conn):
    logging.info("Starting currency pipeline")

    run_currency_ingestion(conn)
    inserted, updated = load_currency_fact(conn)

    log_pipeline(
        conn,
        pipeline_name="currency_pipeline",
        inserted=inserted,
        updated=updated,
        status="SUCCESS"
    )

    logging.info(f"Currency pipeline completed | inserted={inserted}, updated={updated}")
    print("Currency pipeline completed")

def run_pipeline():
    conn = get_connection()

    try:
        logging.info("=== PIPELINE STARTED ===")
        print("Starting Data Warehouse Pipeline...\n")

        run_fuel_pipeline(conn)
        run_oil_pipeline(conn)
        run_currency_pipeline(conn)

        logging.info("=== ALL PIPELINES COMPLETED SUCCESSFULLY ===")
        print("\nALL PIPELINES COMPLETED SUCCESSFULLY")

    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        print(f"Pipeline failed: {e}")

    finally:
        conn.close()
        logging.info("Connection closed")

if __name__ == "__main__":
    run_pipeline()