from urllib import request
from pathlib import Path
import argparse

from google.cloud import storage
import pandas as pd

FILES_PATH = "data/fhv/"
BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/"
PROJECT_ID = "de-zoomcamp-375618 "
BUCKET_NAME = "dtc_data_lake_de-zoomcamp-375618"
TAXI_DATA_YEAR = "2019"


def download_files(files: list) -> None:
    if not Path(FILES_PATH).exists():
        Path(FILES_PATH).mkdir(parents=True)

    for file in files:
        file_path = FILES_PATH + file
        url = BASE_URL + file
        if not Path(file_path).exists():
            print(f"Downloading {url}")
            request.urlretrieve(url=url, filename=file_path)


def convert_to_parquet(file_path: str) -> str:
    file_pq_path = file_path.replace(".csv.gz", ".parquet")

    dtypes = {
        "dispatching_base_num": "string",
        "pickup_datetime": "string",
        "dropOff_datetime": "string",
        "PUlocationID": "Int64",
        "DOlocationID": "Int64",
        "SR_Flag": "string",
        "Affiliated_base_number": "string",
    }

    if not Path(file_pq_path).exists():
        df = pd.read_csv(
            file_path,
            dtype=dtypes,
            parse_dates=["pickup_datetime", "dropOff_datetime"],
        )
        df.to_parquet(file_pq_path, compression="gzip")

    return file_pq_path


def upload_to_gcs(files: list, to_parquet: False) -> None:
    storage_client = storage.Client(PROJECT_ID)
    bucket = storage_client.bucket(BUCKET_NAME)

    for file in files:
        file_path = FILES_PATH + file

        if to_parquet:
            file_path = convert_to_parquet(file_path)

        blob = bucket.blob(file_path)

        if not blob.exists():
            print(f"Uploading {file_path} to GCS")
            blob.upload_from_filename(file_path)


def get_args() -> dict:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--to-parquet", action="store_true", help="convert csv.gz files to parquet"
    )

    return vars(parser.parse_args())


def main() -> None:
    args = get_args()
    files = [f"fhv_tripdata_{TAXI_DATA_YEAR}-{num:02}.csv.gz" for num in range(1, 13)]
    
    download_files(files)
    upload_to_gcs(files, args["to_parquet"])


if __name__ == "__main__":
    main()
