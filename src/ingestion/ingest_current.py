import click
import polars as pl
from sqlalchemy import create_engine
from tqdm import tqdm


def ingest_data(url: str, engine, target_table: str, chunksize: int):
    df_iter = pl.scan_csv(
        url,
        separator=";",
        try_parse_dates=True,
        schema_overrides={"LINIEN_ID": pl.String},
    ).collect_batches(chunk_size=chunksize)

    print("Downloading data...")

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
@click.option("--version", default=2, type=int, help="Dataset version")
@click.option("--chunksize", default=None, type=int, help="Chunk size for ingestion")
@click.option("--target-table", default="stop_event_ingest", help="Target table name")
def main(
    pg_user,
    pg_pass,
    pg_host,
    pg_port,
    pg_db,
    version,
    chunksize,
    target_table,
):
    engine = create_engine(
        f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    )

    url = (
        "https://data.opentransportdata.swiss/en/dataset/ist-daten-v2/permalink"
        if version == 2
        else "https://data.opentransportdata.swiss/en/dataset/istdaten/permalink"
    )

    ingest_data(url=url, engine=engine, target_table=target_table, chunksize=chunksize)


if __name__ == "__main__":
    main()
