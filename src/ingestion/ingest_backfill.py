import sys

sys.path.append("../")
import glob
import shutil
from pathlib import Path
from zipfile import ZipFile

import click
import requests
from tqdm import tqdm

import util.db_handler as db


def get_source_url(year: int, month: int):
    file_name = f"ist-daten-v2-{year}-{month:02d}.zip"

    buckets = [
        f"https://archive.opentransportdata.swiss/istdaten/{year}/",
        "https://archive.opentransportdata.swiss/istdaten/2025/",
    ]

    for bucket in buckets:
        url = bucket + file_name
        response = requests.head(url)
        if response.ok:
            return url


def download_batch(url: str, path: Path):
    r = requests.get(url, stream=True)
    if not r.ok:
        return False

    shutil.rmtree(path, ignore_errors=True)
    path.mkdir(exist_ok=True)

    byte_size = int(r.headers.get("content-length", 0))
    prog = tqdm(
        total=byte_size,
        unit="iB",
        unit_scale=True,
        unit_divisor=1024,
        desc="Downloading data...",
    )

    with open(path / "tmp.zip", "wb") as fd:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            fd.write(chunk)
            prog.update(len(chunk))
    prog.close()

    print("Extracting files")
    with ZipFile(path / "tmp.zip") as zf:
        zf.extractall(path)
    print("Done extracting")


@click.command()
@click.option("--pg-user", default="root", help="PostgreSQL username")
@click.option("--pg-pass", default="root", help="PostgreSQL password")
@click.option("--pg-host", default="localhost", help="PostgreSQL host")
@click.option("--pg-port", default="5432", help="PostgreSQL port")
@click.option("--pg-db", default="swiss_transport", help="PostgreSQL database name")
@click.option("--year", default="2026", type=int, help="Year of the data")
@click.option("--month", default=1, type=int, help="Month of the data")
def main(
    pg_user,
    pg_pass,
    pg_host,
    pg_port,
    pg_db,
    year,
    month,
):
    url = get_source_url(year, month)

    if url is None:
        print(f"No data source found for {year}-{month:02d}")
        return

    data_dir = Path("./data/")

    download_batch(url, data_dir)

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

    for file in tqdm(glob.iglob(str(data_dir / "*.csv")), "Ingesting files"):
        with open(file, "r", encoding="utf-8") as f:
            db.ingest_csv(connection, f)

    connection.close()

    shutil.rmtree(data_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
