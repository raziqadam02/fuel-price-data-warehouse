def load_fuel_fact(conn):
    cursor = conn.cursor()

    insert_sql = """
    INSERT INTO FUEL_DB.ANALYTICS.fact_fuel_price (
        date_key,
        fuel_type_key,
        price,
        source,
        ingestion_time
    )
    SELECT
        d.date_key,
        t.fuel_type_key,
        r.price,
        'fuel_api',
        r.ingestion_time
    FROM (
    
        SELECT 
            TO_DATE(date) AS date,
            'RON95' AS fuel_type,
            ron95 AS price,
            ingestion_time
        FROM FUEL_DB.RAW.fuel_price_raw

        UNION ALL

        SELECT 
            TO_DATE(date) AS date,
            'RON97' AS fuel_type,
            ron97 AS price,
            ingestion_time
        FROM FUEL_DB.RAW.fuel_price_raw

        UNION ALL

        SELECT 
            TO_DATE(date) AS date,
            'DIESEL' AS fuel_type,
            diesel AS price,
            ingestion_time
        FROM FUEL_DB.RAW.fuel_price_raw

    ) r

    JOIN FUEL_DB.ANALYTICS.dim_date d
        ON TO_DATE(r.date) = TO_DATE(d.date_key)

    JOIN FUEL_DB.ANALYTICS.dim_fuel_type t
        ON UPPER(r.fuel_type) = UPPER(t.fuel_type);
    """

    cursor.execute(insert_sql)
    cursor.close()
    return 0, 0