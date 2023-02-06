from pathlib import Path

import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

COLOR = "green"
YEAR = 2020
MONTH = 11
GSC_BLOCK_NAME = "zoom-gsc"
TAXI_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"


@task(log_prints=True, retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame."""

    dtypes = {
        "lpep_pickup_datetime": "string",
        "lpep_dropoff_datetime": "string",
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
        parse_dates=["lpep_pickup_datetime", "lpep_dropoff_datetime"],
    )
    
    return df


@task()
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file."""
    if not Path(f"data/{COLOR}").exists():
        Path(f"data/{COLOR}").mkdir(parents=True)

    path = Path(f"data/{COLOR}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")

    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS."""
    gcp_block = GcsBucket.load(GSC_BLOCK_NAME)
    gcp_block.upload_from_path(from_path=path, to_path=path)


@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function."""
    dataset_file = f"{COLOR}_tripdata_{YEAR}-{MONTH:02}"
    dataset_url = f"{TAXI_URL}/{COLOR}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    path = write_local(df, dataset_file)
    write_gcs(path)
    
    print(f"Number of processed rows: {len(df)}")


if __name__ == "__main__":
    etl_web_to_gcs()
