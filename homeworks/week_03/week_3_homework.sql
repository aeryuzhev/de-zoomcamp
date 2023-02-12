-- -------------------------------------------------------------------------------------

create or replace external table 
  `de-zoomcamp-375618.trips_data_all.ext_fhv_ny_taxi`
options (
  format = 'CSV',
  uris = ['gs://dtc_data_lake_de-zoomcamp-375618/data/fhv/fhv_tripdata_2019-*.csv.gz']
);

-- -------------------------------------------------------------------------------------

create or replace table 
  `de-zoomcamp-375618.trips_data_all.fhv_ny_taxi` as
select
  *
from
  `de-zoomcamp-375618.trips_data_all.ext_fhv_ny_taxi`;

-- -------------------------------------------------------------------------------------

select
  count(distinct Affiliated_base_number)
from
  `trips_data_all.ext_fhv_ny_taxi`;

select
  count(distinct Affiliated_base_number)
from
  `trips_data_all.fhv_ny_taxi`;

-- -------------------------------------------------------------------------------------

select
  count(*)
from
  `trips_data_all.fhv_ny_taxi`
where
  PUlocationID is null
  and DOlocationID is null;

-- -------------------------------------------------------------------------------------

create or replace table 
  `de-zoomcamp-375618.trips_data_all.fhv_ny_taxi_partitioned` 
partition by
  date(pickup_datetime)
cluster by
  Affiliated_base_number as
select 
  * 
from
  `de-zoomcamp-375618.trips_data_all.ext_fhv_ny_taxi`;

-- -------------------------------------------------------------------------------------

select distinct
  Affiliated_base_number
from
  `trips_data_all.fhv_ny_taxi`
where
  date(pickup_datetime) between '2019-03-01' and '2019-03-31';

select distinct
  Affiliated_base_number
from
  `trips_data_all.fhv_ny_taxi_partitioned`
where
  date(pickup_datetime) between '2019-03-01' and '2019-03-31';

-- -------------------------------------------------------------------------------------