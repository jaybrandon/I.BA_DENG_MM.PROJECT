import shutil
from pathlib import Path
from zipfile import ZipFile

import click
import polars as pl
import requests
from sqlalchemy import create_engine
from tqdm import tqdm


def get_source_url(year: int, month: int, version: int):
    file_name = f"ist-daten-{'v2-' if version == 2 else ''}{year}-{month:02d}.zip"

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


def ingest_data(path: Path, engine, target_table: str, chunksize: int):
    df_iter = pl.scan_csv(
        path / "*.csv",
        separator=";",
        try_parse_dates=True,
        schema_overrides={"LINIEN_ID": pl.String},
    ).collect_batches(chunk_size=chunksize)

    first_chunk = next(df_iter)

    first_chunk.write_database(target_table, engine, if_table_exists="replace")

    for df_chunk in tqdm(df_iter, desc="inserting"):
        df_chunk.write_database(target_table, engine, if_table_exists="append")

    print(f"done ingesting to {target_table}")


@click.command()
@click.option("--pg-user", default="root", help="PostgreSQL username")
@click.option("--pg-pass", default="root", help="PostgreSQL password")
@click.option("--pg-host", default="localhost", help="PostgreSQL host")
@click.option("--pg-port", default="5432", help="PostgreSQL port")
@click.option("--pg-db", default="swiss_transport", help="PostgreSQL database name")
@click.option("--year", default="2026", type=int, help="Year of the data")
@click.option("--month", default=1, type=int, help="Month of the data")
@click.option("--version", default=2, type=int, help="Dataset version")
@click.option("--chunksize", default=None, type=int, help="Chunk size for ingestion")
@click.option("--target-table", default="stop_event_ingest", help="Target table name")
def main(
    pg_user,
    pg_pass,
    pg_host,
    pg_port,
    pg_db,
    year,
    month,
    version,
    chunksize,
    target_table,
):
    engine = create_engine(
        f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    )

    url = get_source_url(year, month, version)

    if url is None:
        print(f"No data source found for {year}-{month:02d} version {version}")
        return

    data_dir = Path("./data/")

    download_batch(url, data_dir)

    ingest_data(
        path=data_dir, engine=engine, target_table=target_table, chunksize=chunksize
    )


if __name__ == "__main__":
    main()
