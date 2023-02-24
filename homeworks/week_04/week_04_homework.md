# Week 4 Homework

In this homework, we'll use the models developed during the week 4 videos and enhance the already presented dbt project using the already loaded Taxi data for fhv vehicles for year 2019 in our DWH.

This means that in this homework we use the following data [Datasets list](https://github.com/DataTalksClub/nyc-tlc-data/)

* Yellow taxi data - Years 2019 and 2020
* Green taxi data - Years 2019 and 2020
* fhv data - Year 2019.

We will use the data loaded for:

* Building a source table: `stg_fhv_tripdata`
* Building a fact table: `fact_fhv_trips`
* Create a dashboard

If you don't have access to GCP, you can do this locally using the ingested data from your Postgres database
instead. If you have access to GCP, you don't need to do it for local Postgres -
only if you want to.

> **Note**: if your answer doesn't match exactly, select the closest option

## Question 1

**Question:**

>**What is the count of records in the model fact_trips after running all models with the test run variable disabled and filtering for 2019 and 2020 data only (pickup datetime)?**
>You'll need to have completed the ["Build the first dbt models"](https://www.youtube.com/watch?v=UVI30Vxzd6c) video and have been able to run the models via the CLI.
>You should find the views and models for querying in your DWH.

**Solution:**

```bash
# Download, convert to parquet and upload taxi data to GCS.
python etl_web_to_gsc.py
# Upload parquet files to BigQuery.
python etl_gcs_to_bq.py
```

```sql
select
    count(*)
from
    `de-zoomcamp-375618.dbt_production.fact_trips`
where
    extract(year from pickup_datetime) in (2019, 2020);
```

**Files:**

* [etl_web_to_gcs.py](etl_web_to_gcs.py)
* [etl_gcs_to_bq.py](etl_gcs_to_bq.py)

**Answer:**

`61648442`

## Question 2

**Question:**

>**What is the distribution between service type filtering by years 2019 and 2020 data as done in the videos?**
>You will need to complete "Visualising the data" videos, either using [google data studio](https://www.youtube.com/watch?v=39nLTs74A3E) or [metabase](https://www.youtube.com/watch?v=BnLkrA7a6gM).

**Solution:**

![service_type_distribution.png](images/service_type_distribution.png)

**Answer:**

`89.9/10.1`

## Question 3

**Question:**

>**What is the count of records in the model stg_fhv_tripdata after running all models with the test run variable disabled (:false)?**  
>Create a staging model for the fhv data for 2019 and do not add a deduplication step. Run it via the CLI without limits (is_test_run: false).
>Filter records with pickup time in year 2019.

**Solution:**

```sql
select
    count(*)
from
    `de-zoomcamp-375618.dbt_production.stg_fhv_tripdata`
where
    extract(year from pickup_datetime) = 2019;
```

**Answer:**

`43244696`

## Question 4

**Question:**

>**What is the count of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled (:false)?**  
>Create a core model for the stg_fhv_tripdata joining with dim_zones.
>Similar to what we've done in fact_trips, keep only records with known pickup and dropoff locations entries for pickup and dropoff locations.
>Run it via the CLI without limits (is_test_run: false) and filter records with pickup time in year 2019.

**Solution:**

```sql
select
    count(*)
from
    `de-zoomcamp-375618.dbt_production.fact_fhv_trips`
where
    extract(year from pickup_datetime) = 2019;
```

**Answer:**

`22998722`

### Question 5

**Question:**

>**What is the month with the biggest amount of rides after building a tile for the fact_fhv_trips table?**
>Create a dashboard with some tiles that you find interesting to explore the data. One tile should show the amount of trips per month, as done in the videos for fact_trips, based on the fact_fhv_trips table.

* March
* April
* January
* December
