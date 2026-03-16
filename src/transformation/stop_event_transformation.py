import psycopg2
from psycopg2 import OperationalError, Error

def create_connection(db_name: str, db_user: str, db_password: str, db_host: str, db_port: str):
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Connection to PostgreSQL DB successful")
        return connection
    except OperationalError as e:
        print(f"Connection error: {e}")
        return None
    
def execute_query(connection, query: str):
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
        connection.commit()
    except Error as e:
        connection.rollback()
        print(f"Query error: {e}")

if __name__ == "__main__":
    connection = create_connection(
        "swiss_transport",
        "root",
        "root",
        "localhost",
        "5432",
    )

    if connection:

        execute_query(connection, """
        DROP TABLE IF EXISTS stop_event_ingest_altered;
        
        CREATE TABLE stop_event_ingest_altered AS
        SELECT *
        FROM stop_event_ingest;
        """)

        execute_query(connection, """
                      ALTER TABLE stop_event_ingest_altered
                      ADD COLUMN delay_arrival_sec INTEGER,
                      ADD COLUMN delay_departure_sec INTEGER,
                      ADD COLUMN service_hour SMALLINT,
                      ADD COLUMN service_day_of_week SMALLINT,
                      ADD COLUMN is_peak_hour BOOLEAN;
                      """)
        
        execute_query(connection, """
                      UPDATE stop_event_ingest_altered
                      SET delay_arrival_sec =
                      EXTRACT(EPOCH FROM ("AN_PROGNOSE" - "ANKUNFTSZEIT"))::INTEGER
                      WHERE "ANKUNFTSZEIT" IS NOT NULL
                      AND "AN_PROGNOSE" IS NOT NULL;
                        """)
        
        execute_query(connection, """
                      UPDATE stop_event_ingest_altered
                      SET delay_departure_sec =
                      EXTRACT(EPOCH FROM ("AB_PROGNOSE" - "ABFAHRTSZEIT"))::INTEGER
                      WHERE "ABFAHRTSZEIT" IS NOT NULL
                      AND "AB_PROGNOSE" IS NOT NULL;
                """)
        
        execute_query(connection, """
                      UPDATE stop_event_ingest_altered
                      SET service_hour =
                      EXTRACT(HOUR FROM "ANKUNFTSZEIT")::SMALLINT
                      WHERE "ANKUNFTSZEIT" IS NOT NULL
                      """)
        
        execute_query(connection, """
                      UPDATE stop_event_ingest_altered
                      SET service_day_of_week =
                      EXTRACT(ISODOW FROM "ANKUNFTSZEIT")::SMALLINT
                      WHERE "ANKUNFTSZEIT" IS NOT NULL
                      """)
        
        execute_query(connection, """
                      UPDATE stop_event_ingest_altered
                      SET is_peak_hour = service_hour IN (7, 8, 9, 16, 17, 18);
                      """)
        
        
        execute_query(connection, """
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