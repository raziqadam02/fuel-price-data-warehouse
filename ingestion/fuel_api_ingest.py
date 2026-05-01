import pandas as pd

FILE_PATH = "fuelprice.parquet"


# -----------------------------
# 1. EXTRACT (PARQUET LOAD)
# -----------------------------
def fetch_fuel_data():
    df = pd.read_parquet(FILE_PATH)

    # normalize columns
    df.columns = df.columns.str.lower().str.strip()

    return df


# -----------------------------
# 2. TRANSFORM (TYPE CLEANING)
# -----------------------------
def transform_data(df):
    # convert to proper datetime (Snowflake-safe later)
    df['date'] = pd.to_datetime(df['date']).dt.date

    # replace NaN with None (Snowflake compatible)
    df = df.where(pd.notnull(df), None)

    return df


# -----------------------------
# 3. LOAD (FAST BULK INSERT)
# -----------------------------
def load_to_snowflake(conn, df):
    cursor = conn.cursor()

    try:
        # convert dataframe → list of tuples (FASTEST SAFE METHOD)
        data = list(
            df[['date', 'ron95', 'ron97', 'diesel', 'diesel_eastmsia', 'series_type']]
            .itertuples(index=False, name=None)
        )

        cursor.executemany("""
            INSERT INTO FUEL_DB.RAW.fuel_price_raw 
            (date, ron95, ron97, diesel, diesel_eastmsia, series_type, ingestion_time)
            VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        """, data)

        conn.commit()

    finally:
        cursor.close()


# -----------------------------
# 4. PIPELINE RUNNER
# -----------------------------
def run_ingestion(conn):
    df = fetch_fuel_data()
    df = transform_data(df)
    load_to_snowflake(conn, df)

    print("Fuel data ingested successfully (PARQUET - FAST MODE)")