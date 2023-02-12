from urllib import request
from pathlib import Path

from google.cloud import storage

FILES_PATH = "data/fhv/"
BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/"
PROJECT_ID = "de-zoomcamp-375618 "
BUCKET_NAME = "dtc_data_lake_de-zoomcamp-375618"
TAXI_DATA_YEAR = "2019"


def main() -> None:
    files = [f"fhv_tripdata_{TAXI_DATA_YEAR}-{num:02}.csv.gz" for num in range(1, 13)]
    download_files(files)
    upload_to_gcs(files)


def download_files(files: list) -> None:
    if not Path(FILES_PATH).exists():
        Path(FILES_PATH).mkdir(parents=True)

    for file in files:
        file_path = FILES_PATH + file
        url = BASE_URL + file
        if not Path(file_path).exists():
            print(f"Downloading {url}")
            request.urlretrieve(url=url, filename=file_path)


def upload_to_gcs(files: list) -> None:
    storage_client = storage.Client(PROJECT_ID)
    bucket = storage_client.bucket(BUCKET_NAME)
    
    for file in files:
        file_path = FILES_PATH + file
        blob = bucket.blob(file_path)
        if not blob.exists():
            print(f"Uploading {file_path} to GCS")
            blob.upload_from_filename(file_path)


if __name__ == "__main__":
    main()
