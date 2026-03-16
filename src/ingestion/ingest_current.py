import sys

sys.path.append("../")
from io import BytesIO

import click
import requests

import util.db_handler as db


@click.command()
@click.option("--pg-user", default="root", help="PostgreSQL username")
@click.option("--pg-pass", default="root", help="PostgreSQL password")
@click.option("--pg-host", default="localhost", help="PostgreSQL host")
@click.option("--pg-port", default="5432", help="PostgreSQL port")
@click.option("--pg-db", default="swiss_transport", help="PostgreSQL database name")
def main(
    pg_user,
    pg_pass,
    pg_host,
    pg_port,
    pg_db,
):
    url = "https://data.opentransportdata.swiss/en/dataset/ist-daten-v2/permalink"

    r = requests.get(url, stream=True)
    if not r.ok:
        print(f"Failed to get file - Status: {r.status} - URL: {url}")

    connection = db.create_connection(pg_db, pg_user, pg_pass, pg_host, pg_port)

    db.execute_query(
        connection,
        """
        DROP TABLE IF EXISTS stg_stop_events;

        CREATE TABLE stg_stop_events (
            betriebstag VARCHAR,
            fahrt_bezeichner VARCHAR,
            betreiber_id VARCHAR,
            betreiber_abk VARCHAR,
            betreiber_name VARCHAR,
            produkt_id VARCHAR,
            linien_id VARCHAR,
            linien_text VARCHAR,
            umlauf_id VARCHAR,
            verkehrsmittel_text VARCHAR,
            zusatzfahrt_tf BOOLEAN,
            faellt_aus_tf BOOLEAN,
            bpuic VARCHAR,
            haltestellen_name VARCHAR,
            ankunftszeit VARCHAR,
            an_prognose VARCHAR,
            an_prognose_status VARCHAR,
            abfahrtszeit VARCHAR,
            ab_prognose VARCHAR,
            ab_prognose_status VARCHAR,
            durchfahrt_tf BOOLEAN,
            sloid VARCHAR
        );
        """,
    )

    print("Ingesting data...")
    db.ingest_csv(connection, BytesIO(r.content))


if __name__ == "__main__":
    main()
