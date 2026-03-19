import psycopg2
from psycopg2 import OperationalError, Error

import util.db_handler as db

def main():
    connection = db.create_connection(
        "swiss_transport",
        "root",
        "root",
        "localhost",
        "5432",
    )

    if connection:

        db.execute_query(connection, """
        DROP TABLE IF EXISTS stop_event_ingest_altered;
        
        CREATE TABLE stop_event_ingest_altered AS
        SELECT *
        FROM stop_event_staging;
        """)

        db.execute_query(connection, """
                      ALTER TABLE stop_event_ingest_altered
                      ADD COLUMN delay_arrival_sec INTEGER,
                      ADD COLUMN delay_departure_sec INTEGER,
                      ADD COLUMN service_hour SMALLINT,
                      ADD COLUMN service_day_of_week SMALLINT,
                      ADD COLUMN is_peak_hour BOOLEAN;
                      """)
        
        db.execute_query(connection, """
                      UPDATE stop_event_ingest_altered
                      SET delay_arrival_sec =
                      EXTRACT(EPOCH FROM ("AN_PROGNOSE" - "ANKUNFTSZEIT"))::INTEGER
                      WHERE "ANKUNFTSZEIT" IS NOT NULL
                      AND "AN_PROGNOSE" IS NOT NULL;
                        """)
        
        db.execute_query(connection, """
                      UPDATE stop_event_ingest_altered
                      SET delay_departure_sec =
                      EXTRACT(EPOCH FROM ("AB_PROGNOSE" - "ABFAHRTSZEIT"))::INTEGER
                      WHERE "ABFAHRTSZEIT" IS NOT NULL
                      AND "AB_PROGNOSE" IS NOT NULL;
                """)
        
        db.execute_query(connection, """
                      UPDATE stop_event_ingest_altered
                      SET service_hour =
                      EXTRACT(HOUR FROM "ANKUNFTSZEIT")::SMALLINT
                      WHERE "ANKUNFTSZEIT" IS NOT NULL
                      """)
        
        db.execute_query(connection, """
                      UPDATE stop_event_ingest_altered
                      SET service_day_of_week =
                      EXTRACT(ISODOW FROM "ANKUNFTSZEIT")::SMALLINT
                      WHERE "ANKUNFTSZEIT" IS NOT NULL
                      """)
        
        db.execute_query(connection, """
                      UPDATE stop_event_ingest_altered
                      SET is_peak_hour = service_hour IN (7, 8, 9, 16, 17, 18);
                      """)
        
        
        db.execute_query(connection, """
                    DROP TABLE IF EXISTS station_delay_daily;

                    CREATE TABLE station_delay_daily AS
                    SELECT
                        "HALTESTELLEN_NAME",
                        DATE("ANKUNFTSZEIT") AS service_date,
                        EXTRACT(ISODOW FROM "ANKUNFTSZEIT")::SMALLINT AS day_of_week,
                        COUNT(*) AS total_records,
                        COUNT(delay_arrival_sec) AS records_with_delay,
                        AVG(delay_arrival_sec)::DOUBLE PRECISION AS avg_delay_arrival_sec,
                        SUM(delay_arrival_sec)::BIGINT AS total_delay_seconds,
                        AVG(CASE WHEN delay_arrival_sec > 0 THEN 1.0 ELSE 0.0 END)::DOUBLE PRECISION AS delay_percentage
                    FROM stop_event_ingest_altered
                    WHERE "ANKUNFTSZEIT" IS NOT NULL
                    GROUP BY
                        "HALTESTELLEN_NAME",
                        DATE("ANKUNFTSZEIT"),
                        EXTRACT(ISODOW FROM "ANKUNFTSZEIT");
        """)
                
    connection.close()

if __name__ == "__main__":
    main()