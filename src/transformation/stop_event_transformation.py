from src.util import db_handler as db


def main():
    connection = db.create_connection(
        "swiss_transport",
        "root",
        "root",
        "localhost",
        "5432",
    )

    if connection:
        print("Preparing final table for fill/refresh")

        db.execute_query(
            connection,
            """
                    
                    CREATE TABLE IF NOT EXISTS fact_stop_events (
                    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    event_unique_id TEXT UNIQUE,
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
            """,
        )

        print("filling/refreshing final table...")

        db.execute_query(
            connection,
            """
                         
                    BEGIN;

                    SET LOCAL synchronous_commit = OFF;

                    SET LOCAL work_mem = '512MB';

                    DELETE FROM fact_stop_events
                    WHERE betriebstag IN (
                        SELECT DISTINCT TO_DATE(NULLIF(betriebstag, ''), 'DD.MM.YYYY')
                        FROM stg_stop_events
                        WHERE betriebstag IS NOT NULL
                    );

                    INSERT INTO fact_stop_events (
                        event_unique_id,
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
                        sloid,
                        delay_arrival_sec,
                        delay_departure_sec,
                        service_hour,
                        service_day_of_week,
                        is_peak_hour
                    )
                    WITH parsed AS (
                        SELECT
                            CONCAT_WS(
                                '|',
                                NULLIF(fahrt_bezeichner, ''),
                                NULLIF(bpuic, ''),
                                NULLIF(betriebstag, ''),
                                NULLIF(ankunftszeit, ''),
                                NULLIF(abfahrtszeit, '')
                            ) AS event_unique_id,
                            TO_DATE(NULLIF(betriebstag, ''), 'DD.MM.YYYY') AS betriebstag,
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
                            TO_TIMESTAMP(NULLIF(ankunftszeit, ''), 'DD.MM.YYYY HH24:MI') AS ankunftszeit_ts,
                            TO_TIMESTAMP(NULLIF(an_prognose, ''), 'DD.MM.YYYY HH24:MI') AS an_prognose_ts,
                            an_prognose_status,
                            TO_TIMESTAMP(NULLIF(abfahrtszeit, ''), 'DD.MM.YYYY HH24:MI') AS abfahrtszeit_ts,
                            TO_TIMESTAMP(NULLIF(ab_prognose, ''), 'DD.MM.YYYY HH24:MI') AS ab_prognose_ts,
                            ab_prognose_status,
                            durchfahrt_tf,
                            sloid
                        FROM stg_stop_events
                        WHERE LOWER(produkt_id) = 'zug'
                    ),
                    deduplicated AS (
                        SELECT *, ROW_NUMBER() OVER (PARTITION BY event_unique_id ORDER BY ankunftszeit_ts DESC) as row_num
                        FROM parsed
                    )
                    SELECT
                        event_unique_id,
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
                        ankunftszeit_ts,
                        an_prognose_ts,
                        an_prognose_status,
                        abfahrtszeit_ts,
                        ab_prognose_ts,
                        ab_prognose_status,
                        durchfahrt_tf,
                        sloid,
                        CASE
                            WHEN ankunftszeit_ts IS NOT NULL AND an_prognose_ts IS NOT NULL
                            THEN NULLIF(GREATEST(0, EXTRACT(EPOCH FROM (ankunftszeit_ts - an_prognose_ts))::INTEGER), 0)
                            ELSE NULL
                        END AS delay_arrival_sec,
                        CASE
                            WHEN abfahrtszeit_ts IS NOT NULL AND ab_prognose_ts IS NOT NULL
                            THEN NULLIF(GREATEST(0, EXTRACT(EPOCH FROM (abfahrtszeit_ts - ab_prognose_ts))::INTEGER), 0)
                            ELSE NULL
                        END AS delay_departure_sec,
                        CASE
                            WHEN ankunftszeit_ts IS NOT NULL
                            THEN EXTRACT(HOUR FROM ankunftszeit_ts)::SMALLINT
                            ELSE NULL
                        END AS service_hour,
                        CASE
                            WHEN ankunftszeit_ts IS NOT NULL
                            THEN EXTRACT(ISODOW FROM ankunftszeit_ts)::SMALLINT
                            ELSE NULL
                        END AS service_day_of_week,
                        CASE
                            WHEN ankunftszeit_ts IS NOT NULL
                            THEN EXTRACT(HOUR FROM ankunftszeit_ts) IN (7, 8, 9, 16, 17, 18)
                            ELSE NULL
                        END AS is_peak_hour
                    FROM deduplicated
                    WHERE row_num = 1;

                    COMMIT;
                """,
        )

        print("Creating/refreshing station delay aggregation...")

        db.execute_query(
            connection,
            """
                         
                        DROP TABLE IF EXISTS station_delay_daily;

                        CREATE TABLE station_delay_daily AS
                        SELECT
                            haltestellen_name,
                            betriebstag AS service_date,
                            EXTRACT(ISODOW FROM betriebstag)::SMALLINT AS day_of_week,
                            COUNT(*) AS total_records,
                            SUM(CASE WHEN delay_arrival_sec > 0 THEN 1 ELSE 0 END)::INTEGER AS records_with_delay,
                            AVG(delay_arrival_sec)::DOUBLE PRECISION AS avg_delay_seconds,
                            SUM(delay_arrival_sec)::BIGINT AS total_delay_seconds,
                            AVG(
                                CASE
                                    WHEN delay_arrival_sec > 0 THEN 1.0
                                    ELSE 0.0
                                END
                            )::DOUBLE PRECISION AS delay_percentage
                        FROM fact_stop_events
                        WHERE ankunftszeit IS NOT NULL
                        GROUP BY
                            haltestellen_name,
                            betriebstag;
        """,
        )

        connection.close()


if __name__ == "__main__":
    main()
