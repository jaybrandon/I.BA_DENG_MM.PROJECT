import psycopg2
from psycopg2 import Error, OperationalError


def create_connection(
    db_name: str, db_user: str, db_password: str, db_host: str, db_port: str
):
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


def ingest_csv(connection, buffer):
    try:
        with connection.cursor() as cursor:
            copy_sql = """
                COPY stg_stop_events 
                FROM STDIN 
                WITH CSV HEADER DELIMITER ';'
            """
            cursor.copy_expert(copy_sql, buffer)
        connection.commit()
    except Error as e:
        connection.rollback()
        print(f"Query error: {e}")
