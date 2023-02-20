from pathlib import Path

import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

GSC_BLOCK_NAME = "zoom-gsc"
TAXI_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"


@task(retries=3)
def fetch(dataset_url: str, color: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame."""
    pickup_dt_column = "tpep_pickup_datetime"
    dropoff_dt_column = "tpep_dropoff_datetime"    
    
    if color == "green":
        pickup_dt_column = "lpep_pickup_datetime"
        dropoff_dt_column = "lpep_dropoff_datetime"     

    dtypes = {
        pickup_dt_column: "object",
        dropoff_dt_column: "object",
        "passenger_count": "float64",
        "trip_distance": "float64",
        "RatecodeID": "float64",
        "store_and_fwd_flag": "object",
        "PULocationID": "int64",
        "DOLocationID": "int64",
        "payment_type": "float64",
        "fare_amount": "float64",
        "extra": "float64",
        "mta_tax": "float64",
        "tip_amount": "float64",
        "tolls_amount": "float64",
        "improvement_surcharge": "float64",
        "total_amount": "float64",
        "congestion_surcharge": "float64",
    }

    df = pd.read_csv(
        dataset_url,
        dtype=dtypes,
        parse_dates=[pickup_dt_column, dropoff_dt_column],
    )

    return df


@task()
def write_local(df: pd.DataFrame, dataset_file: str, color: str) -> Path:
    """Write DataFrame out locally as parquet file."""
    if not Path(f"data/{color}").exists():
        Path(f"data/{color}").mkdir(parents=True)

    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")

    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS."""
    gcp_block = GcsBucket.load(GSC_BLOCK_NAME)
    gcp_block.upload_from_path(from_path=path, to_path=path)


@flow()
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """The main ETL function."""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"{TAXI_URL}/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url, color)
    path = write_local(df, dataset_file, color)
    write_gcs(path)

    return len(df)


@flow(log_prints=True)
def etl_parent_flow(
    year: int = 2021, months: list[int] = [1, 2], color: str = "yellow"
) -> None:
    """Parent function for main ETL function."""
    proc_rows_count = 0

    for month in months:
        proc_rows_count += etl_web_to_gcs(year, month, color)

    print(f"Number of processed rows: {proc_rows_count}")


if __name__ == "__main__":
    year = 2020
    months = [1]
    color = "yellow"

    etl_parent_flow(year, months, color)
