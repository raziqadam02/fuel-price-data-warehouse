import requests
import pandas as pd

# =========================
# API URL
# =========================
URL = "https://open.er-api.com/v6/latest/USD"


# =========================
# 1. FETCH DATA
# =========================
def fetch_currency_data():
    response = requests.get(URL)

    if response.status_code != 200:
        raise Exception("Currency API request failed")

    return response.json()


# =========================
# 2. TRANSFORM DATA
# =========================
def transform_data(data):
    rates = data.get("rates", {})

    df = pd.DataFrame(list(rates.items()), columns=["currency", "rate"])

    print("Currency preview:")
    print(df.head())

    return df


# =========================
# 3. LOAD TO SNOWFLAKE
# =========================
def load_to_snowflake(conn, df):
    cursor = conn.cursor()

    data = [
        (
            row['currency'],
            row['rate']
        )
        for _, row in df.iterrows()
    ]

    cursor.executemany("""
        INSERT INTO FUEL_DB.RAW.currency_rate_raw
        (currency, rate, ingestion_time)
        VALUES (%s, %s, CURRENT_TIMESTAMP)
    """, data)

    conn.commit()
    cursor.close()

    print(f"Inserted {len(data)} currency rows into Snowflake")


# =========================
# 4. PIPELINE ENTRY POINT (IMPORTANT)
# =========================
def run_ingestion(conn):
    print("🚀 Starting Currency Pipeline...")

    data = fetch_currency_data()
    df = transform_data(data)

    if df.empty:
        print("No currency data found")
        return

    load_to_snowflake(conn, df)

    print("✅ Currency ingestion completed successfully")


# =========================
# OPTIONAL LOCAL TEST MODE
# =========================
if __name__ == "__main__":
    from ingestion.snowflake_connection import get_connection

    conn = get_connection()
    run_ingestion(conn)
    conn.close()