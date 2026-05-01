def load_currency_fact(conn):
    cursor = conn.cursor()

    insert_sql = """
    INSERT INTO FUEL_DB.ANALYTICS.fact_currency_rate (
        date_key,
        currency_key,
        rate,
        source,
        ingestion_time
    )
    SELECT
        CURRENT_DATE,
        d.currency_key,
        r.rate,
        'currency_api',
        r.ingestion_time
    FROM FUEL_DB.RAW.currency_rate_raw r
    JOIN FUEL_DB.ANALYTICS.dim_currency d
        ON UPPER(TRIM(r.currency)) = UPPER(TRIM(d.currency_code));
    """

    cursor.execute(insert_sql)

    return 0, 0