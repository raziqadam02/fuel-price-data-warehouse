import pandas as pd

FILE_PATH = "fuelprice.parquet"

def fetch_fuel_data():
    df = pd.read_parquet(FILE_PATH)

    df.columns = df.columns.str.lower().str.strip()

    return df

def transform_data(df):
    df['date'] = pd.to_datetime(df['date']).dt.date

    df = df.where(pd.notnull(df), None)

    return df

def load_to_snowflake(conn, df):
    cursor = conn.cursor()

    try:
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

def run_ingestion(conn):
    df = fetch_fuel_data()
    df = transform_data(df)
    load_to_snowflake(conn, df)

    print("Fuel data ingested successfully")