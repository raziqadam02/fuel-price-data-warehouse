def load_oil_fact(conn):
    cursor = conn.cursor()

    insert_sql = """
    INSERT INTO FUEL_DB.ANALYTICS.fact_oil_price (
        date_key,
        product,
        price,
        source,
        ingestion_time
    )
    SELECT
        d.date_key,
        r.product,
        r.price,
        'oil_api',
        r.ingestion_time
    FROM FUEL_DB.RAW.oil_price_raw r
    JOIN FUEL_DB.ANALYTICS.dim_date d
        ON r.date = d.date_key;
    """

    cursor.execute(insert_sql)

    return 0, 0