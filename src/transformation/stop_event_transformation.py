import src.util.db_handler as db

def main():
    connection = db.create_connection(
        "swiss_transport",
        "root",
        "root",
        "localhost",
        "5432",
    )

    if connection:
        
        print("Creating final table...")
        db.execute_query(connection, """
                    DROP TABLE IF EXISTS fact_stop_events;
                    
                    CREATE TABLE fact_stop_events (
                    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    betriebstag DATE,
                    fahrt_bezeichner TEXT,
                    betreiber_id TEXT,
                    betreiber_abk TEXT,
                    betreiber_name TEXT,
                    produkt_id TEXT,
                    linien_id TEXT,
                    linien_text TEXT,
                    umlauf_id TEXT,
                    verkehrsmittel_text TEXT,
                    zusatzfahrt_tf BOOLEAN,
                    faellt_aus_tf BOOLEAN,
                    bpuic TEXT,
                    haltestellen_name TEXT,
                    ankunftszeit TIMESTAMP,
                    an_prognose TIMESTAMP,
                    an_prognose_status TEXT,
                    abfahrtszeit TIMESTAMP,
                    ab_prognose TIMESTAMP,
                    ab_prognose_status TEXT,
                    durchfahrt_tf BOOLEAN,
                    sloid TEXT,
                    delay_arrival_sec INTEGER,
                    delay_departure_sec INTEGER,
                    service_hour SMALLINT,
                    service_day_of_week SMALLINT,
                    is_peak_hour BOOLEAN
                );
            """)
        

        db.execute_query(connection, """
            INSERT INTO fact_stop_events (
                betriebstag,
                fahrt_bezeichner,
                betreiber_id,
                betreiber_abk,
                betreiber_name,
                produkt_id,
                linien_id,
                linien_text,
                umlauf_id,
                verkehrsmittel_text,
                zusatzfahrt_tf,
                faellt_aus_tf,
                bpuic,
                haltestellen_name,
                ankunftszeit,
                an_prognose,
                an_prognose_status,
                abfahrtszeit,
                ab_prognose,
                ab_prognose_status,
                durchfahrt_tf,
                sloid
            )
            SELECT
                TO_DATE(NULLIF(betriebstag, ''), 'DD.MM.YYYY'),
                fahrt_bezeichner,
                betreiber_id,
                betreiber_abk,
                betreiber_name,
                produkt_id,
                linien_id,
                linien_text,
                umlauf_id,
                verkehrsmittel_text,
                zusatzfahrt_tf,
                faellt_aus_tf,
                bpuic,
                haltestellen_name,
                TO_TIMESTAMP(NULLIF(ankunftszeit, ''), 'DD.MM.YYYY HH24:MI'),
                TO_TIMESTAMP(NULLIF(an_prognose, ''), 'DD.MM.YYYY HH24:MI'),
                an_prognose_status,
                TO_TIMESTAMP(NULLIF(abfahrtszeit, ''), 'DD.MM.YYYY HH24:MI'),
                TO_TIMESTAMP(NULLIF(ab_prognose, ''), 'DD.MM.YYYY HH24:MI'),
                ab_prognose_status,
                durchfahrt_tf,
                sloid
            FROM stg_stop_events;
        """)
        
        db.execute_query(connection, """
                      UPDATE fact_stop_events
                      SET delay_arrival_sec =
                      EXTRACT(EPOCH FROM ("an_prognose" - "ankunftszeit"))::INTEGER
                      WHERE "ankunftszeit" IS NOT NULL
                      AND "an_prognose" IS NOT NULL;
                        """)
        
        db.execute_query(connection, """
                      UPDATE fact_stop_events
                      SET delay_departure_sec =
                      EXTRACT(EPOCH FROM ("ab_prognose" - "abfahrtszeit"))::INTEGER
                      WHERE "abfahrtszeit" IS NOT NULL
                      AND "ab_prognose" IS NOT NULL;
                """)
        
        db.execute_query(connection, """
                      UPDATE fact_stop_events
                      SET service_hour =
                      EXTRACT(HOUR FROM "ankunftszeit")::SMALLINT
                      WHERE "ankunftszeit" IS NOT NULL;
                      """)
        
        db.execute_query(connection, """
                      UPDATE fact_stop_events
                      SET service_day_of_week =
                      EXTRACT(ISODOW FROM "ankunftszeit")::SMALLINT
                      WHERE "ankunftszeit" IS NOT NULL;
                      """)
        
        db.execute_query(connection, """
                      UPDATE fact_stop_events
                      SET is_peak_hour = service_hour IN (7, 8, 9, 16, 17, 18);
                      """)
        
        print("Creating station delay aggregation...")
        db.execute_query(connection, """
                    DROP TABLE IF EXISTS station_delay_daily;

                    CREATE TABLE station_delay_daily AS
                    SELECT
                        "haltestellen_name",
                        DATE("ankunftszeit") AS service_date,
                        EXTRACT(ISODOW FROM "ankunftszeit")::SMALLINT AS day_of_week,
                        COUNT(*) AS total_records,
                        COUNT(delay_arrival_sec) AS records_with_delay,
                        AVG(delay_arrival_sec)::DOUBLE PRECISION AS avg_delay_arrival_sec,
                        SUM(delay_arrival_sec)::BIGINT AS total_delay_seconds,
                        AVG(CASE WHEN delay_arrival_sec > 0 THEN 1.0 ELSE 0.0 END)::DOUBLE PRECISION AS delay_percentage
                    FROM fact_stop_events
                    WHERE "ankunftszeit" IS NOT NULL
                    GROUP BY
                        "haltestellen_name",
                        DATE("ankunftszeit"),
                        EXTRACT(ISODOW FROM "ankunftszeit");
        """)
                
        connection.close()

if __name__ == "__main__":
    main()