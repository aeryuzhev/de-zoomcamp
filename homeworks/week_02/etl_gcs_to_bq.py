from pathlib import Path

import pandas as pd
from prefect import flow, task
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

GSC_BLOCK_NAME = "zoom-gsc"
GCP_CREDS_BLOCK_NAME = "zoom-gcp-creds"
CHUNKSIZE = 100_000
DEST_TABLE = "de-zoomcamp-375618.trips_data_all.yellow_taxi_trips"
PROJECT_ID = "de-zoomcamp-375618"


@task(retries=3)
def extract_from_gcs(year: int, month: int, color: str) -> Path:
    """Download trip data from GCS."""
    if not Path(f"data/{color}").exists():
        Path(f"data/{color}").mkdir(parents=True)

    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcp_block = GcsBucket.load(GSC_BLOCK_NAME)
    gcp_block.get_directory(from_path=gcs_path, local_path=".")

    return Path(gcs_path)


@task(log_prints=True)
def read_parquet(path: Path) -> pd.DataFrame:
    """Create DataFrame from parquet file."""
    df = pd.read_parquet(path)

    return df


@task()
def write_bq(df: pd.DataFrame, chunksize: int = 100_000) -> None:
    """Write DataFrame to BigQuery."""
    gcp_credentials_block = GcpCredentials.load(GCP_CREDS_BLOCK_NAME)

    df.to_gbq(
        destination_table=DEST_TABLE,
        project_id=PROJECT_ID,
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=chunksize,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq(year: int, month: int, color: str) -> int:
    """Main ETL flow to load data into Big Query."""
    path = extract_from_gcs(year, month, color)
    df = read_parquet(path)
    write_bq(df, CHUNKSIZE)
    
    return len(df)


@flow(log_prints=True)
def etl_parent_flow(
    year: int = 2021, months: list[int] = [1, 2], color: str = "yellow"
) -> None:
    """Parent function for main ETL function."""
    proc_rows_count = 0

    for month in months:
        proc_rows_count += etl_gcs_to_bq(year, month, color)

    print(f"Number of processed rows: {proc_rows_count}")


if __name__ == "__main__":
    year = 2020
    months = [1]
    color = "yellow"

    etl_parent_flow(year, months, color)
