from urllib import request
from pathlib import Path

import pandas as pd
from google.cloud import storage

BASE_SOURCE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"
GCS_PROJECT_ID = "de-zoomcamp-375618"
GCS_BUCKET_NAME = "dtc_data_lake_de-zoomcamp-375618"


def get_df_date_column_names(source_type: str) -> list:
    date_column_names = []

    if source_type == "yellow":
        date_column_names = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
    elif source_type == "green":
        date_column_names = ["lpep_pickup_datetime", "lpep_dropoff_datetime"]
    elif source_type == "fhv":
        date_column_names = ["pickup_datetime", "dropOff_datetime"]

    return date_column_names


def get_df_dtypes(source_type: str, date_column_names: list[str]) -> dict:
    dtypes = {}

    if source_type in ["yellow", "green"]:
        dtypes = {
            "VendorID": "Int64",
            date_column_names[0]: "object",
            date_column_names[1]: "object",
            "passenger_count": "Int64",
            "trip_distance": "float64",
            "RatecodeID": "Int64",
            "store_and_fwd_flag": "object",
            "PULocationID": "Int64",
            "DOLocationID": "Int64",
            "payment_type": "Int64",
            "fare_amount": "float64",
            "extra": "float64",
            "mta_tax": "float64",
            "tip_amount": "float64",
            "tolls_amount": "float64",
            "improvement_surcharge": "float64",
            "total_amount": "float64",
            "congestion_surcharge": "float64",
        }

        if source_type == "green":
            dtypes.update(
                {
                    "trip_type": "Int64",
                    "ehail_fee": "float64",
                }
            )
    elif source_type == "fhv":
        dtypes = {
            "dispatching_base_num": "string",
            "pickup_datetime": "object",
            "dropOff_datetime": "object",
            "PUlocationID": "Int64",
            "DOlocationID": "Int64",
            "SR_Flag": "string",
            "Affiliated_base_number": "string",
        }

    return dtypes


def make_data_dir(source_type: str) -> str:
    dir_path = f"data/{source_type}"
    if not Path(dir_path).exists():
        Path(dir_path).mkdir(parents=True)

    return dir_path


def extract_source_data(source_type: str, year: int, dir_path: str) -> list[str]:
    csv_file_pathes = []

    for month in range(1, 13):
        file_name = f"{source_type}_tripdata_{year}-{month:02}.csv.gz"
        file_path = f"{dir_path}/{file_name}"
        file_url = f"{BASE_SOURCE_URL}{source_type}/{file_name}"
        csv_file_pathes.append(file_path)

        if not Path(file_path).exists():
            print(f"Downloading csv: {file_url}")
            request.urlretrieve(url=file_url, filename=file_path)

    return csv_file_pathes


def convert_to_parquet(csv_file_pathes: list[str], source_type: str) -> list[str]:
    date_column_names = get_df_date_column_names(source_type)
    dtypes = get_df_dtypes(source_type, date_column_names)
    pq_files_pathes = []

    for file_path in csv_file_pathes:
        pq_file_path = file_path.replace(".csv.gz", ".parquet")
        pq_files_pathes.append(pq_file_path)

        if not Path(pq_file_path).exists():
            print(f"Converting to parquet: {file_path}")
            df = pd.read_csv(file_path, dtype=dtypes, parse_dates=date_column_names)
            df.to_parquet(pq_file_path, compression="gzip")

    return pq_files_pathes


def upload_to_gcs(pq_file_pathes: list[str]) -> None:
    storage_client = storage.Client(GCS_PROJECT_ID)
    bucket = storage_client.bucket(GCS_BUCKET_NAME)

    for file_path in pq_file_pathes:
        blob = bucket.blob(file_path)
        if not blob.exists():
            print(f"Uploading to GCS: {file_path}")
            blob.upload_from_filename(file_path)


def main(source_type: str, year: int) -> None:
    data_dir_path = make_data_dir(source_type)
    csv_file_pathes = extract_source_data(source_type, year, data_dir_path)
    pq_file_pathes = convert_to_parquet(csv_file_pathes, source_type)
    upload_to_gcs(pq_file_pathes)


if __name__ == "__main__":
    params = [
        ("yellow", 2019),
        ("yellow", 2020),
        ("green", 2019),
        ("green", 2020),
        ("fhv", 2019),
    ]

    for param in params:
        main(*param)
