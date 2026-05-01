import requests
import pandas as pd
from ingestion.snowflake_connection import get_connection

# =========================
# API URL
# =========================
URL = "https://api.eia.gov/v2/petroleum/pri/gnd/data/?api_key=NTjxauGZEjYvXNfglyFA69frkaBb6LEZh1zqqjME&frequency=weekly&data[0]=value&start=2003-01-01&end=2026-04-20&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"


# =========================
# 1. FETCH DATA
# =========================
def fetch_oil_data():
    response = requests.get(URL)
    print("Status Code:", response.status_code)

    if response.status_code != 200:
        raise Exception("API request failed")

    return response.json()


# =========================
# 2. TRANSFORM DATA
# =========================
def transform_data(data):
    records = data.get("response", {}).get("data", [])
    print("Number of records:", len(records))

    df = pd.DataFrame(records)

    df = df[['period', 'value', 'product']]
    df.columns = ['date', 'price', 'product']

    df['date'] = pd.to_datetime(df['date']).dt.date

    print("Preview data:")
    print(df.head())

    return df


# =========================
# CLEAN FUNCTION
# =========================
def clean_value(value):
    if pd.isna(value):
        return None
    return value


# =========================
# 3. LOAD TO SNOWFLAKE
# =========================
def load_to_snowflake(conn, df):
    cursor = conn.cursor()

    data = [
        (
            clean_value(row['date']),
            clean_value(row['price']),
            clean_value(row['product'])
        )
        for _, row in df.iterrows()
    ]

    cursor.executemany("""
        INSERT INTO FUEL_DB.RAW.oil_price_raw 
        (date, price, product, ingestion_time)
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
    """, data)

    conn.commit()
    cursor.close()

    print(f"Inserted {len(data)} rows into Snowflake")


# =========================
# 4. PIPELINE ENTRY POINT (THIS IS WHAT YOUR MAIN PIPELINE CALLS)
# =========================
def run_ingestion(conn):
    print("🚀 Starting Oil Data Pipeline...")

    data = fetch_oil_data()
    df = transform_data(data)

    if df.empty:
        print("No data retrieved")
        return

    load_to_snowflake(conn, df)

    print("✅ Oil ingestion completed successfully")


# =========================
# OPTIONAL LOCAL RUN (KEEP FOR TESTING)
# =========================
if __name__ == "__main__":
    conn = get_connection()
    run_ingestion(conn)
    conn.close()