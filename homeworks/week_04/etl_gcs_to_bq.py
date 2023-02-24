from google.cloud import bigquery

GCS_PROJECT_ID = "de-zoomcamp-375618"
GCS_BUCKET_NAME = "dtc_data_lake_de-zoomcamp-375618"
GBQ_DATASET = "trips_data_all"
GCS_BASE_URI = "gs://dtc_data_lake_de-zoomcamp-375618/data"


# def create_bq_ext_table(source_type: str) -> None:
#     """Creates an BigQuery external table using parquet files from GCS as a source data."""
#     client = bigquery.Client(GCS_PROJECT_ID)

#     gcs_uri = f"{GCS_BASE_URI}/{source_type}/{source_type}_tripdata_*.parquet"
#     ext_table = f"{GCS_PROJECT_ID}.{GBQ_DATASET}.{source_type}_tripdata_ext"

#     external_config = bigquery.ExternalConfig("PARQUET")
#     external_config.source_uris = [gcs_uri]
#     external_config.autodetect = True

#     table = bigquery.Table(ext_table)
#     table.external_data_configuration = external_config

#     table = client.create_table(table, exists_ok=True)
#     print(f"Created {table.table_type} {table.full_table_id}.")


def create_bq_table(source_type: str) -> None:
    """Creates a BigQuery table using parquet files from GCS as a source data."""
    client = bigquery.Client(GCS_PROJECT_ID)

    gcs_uri = f"{GCS_BASE_URI}/{source_type}/{source_type}_tripdata_*.parquet"
    ext_table_id = f"{GCS_PROJECT_ID}.{GBQ_DATASET}.{source_type}_tripdata"

    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET)
    job_config.write_disposition = "WRITE_TRUNCATE"

    load_job = client.load_table_from_uri(gcs_uri, ext_table_id, job_config=job_config)
    load_job.result()

    dest_table = client.get_table(ext_table_id)
    print(
        f"Created {dest_table.table_type} {dest_table.full_table_id}.",
        f"Loaded {dest_table.num_rows} rows.",
    )


if __name__ == "__main__":
    params = [
        "yellow",
        "green",
        "fhv",
    ]
    
    for param in params:
        create_bq_table(param)
