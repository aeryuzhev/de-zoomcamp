# Week 1 Homework

In this homework we'll prepare the environment
and practice with Docker and SQL

## Question 1. Knowing docker tags

**Question:**

>Run the command to get information on Docker
>
>```docker --help```
>
>Now run the command to get help on the "docker build" command
>
>Which tag has the following text? - *Write the image ID to the file*

**Answer:**

`--iidfile string`

## Question 2. Understanding docker first run

**Question:**

>Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash.
>Now check the python modules that are installed ( use pip list).
>How many python packages/modules are installed?

**Answer:**

`3`

## Question 3. Count records

**Question:**

>How many taxi trips were totally made on January 15?
>
>Tip: started and finished on 2019-01-15.
>
>Remember that `lpep_pickup_datetime` and `lpep_dropoff_datetime` columns are in the format timestamp (date and hour+min+sec) and not in date.

**Solution:**

```postgres-sql
select 
    count(*)
from 
    green_taxi_trips
where 
    lpep_pickup_datetime::date = '2019-01-15'
    and lpep_dropoff_datetime::date = '2019-01-15';
```

**Answer:**

`20530`

## Question 4. Largest trip for each day

**Question:**

>Which was the day with the largest trip distance
>Use the pick up time for your calculations.

**Solution:**

```postgres-sql
select 
    lpep_pickup_datetime::date as pickup_date,
    trip_distance
from 
    green_taxi_trips
order by
    trip_distance desc
limit
    1;
```

**Answer:**

`2019-01-15`

## Question 5. The number of passengers

**Question:**

>In 2019-01-01 how many trips had 2 and 3 passengers?

**Solution:**

```postgres-sql
select 
    passenger_count,
    count(*)
from 
    green_taxi_trips
where
    lpep_pickup_datetime::date = '2019-01-01'
    and passenger_count in (2, 3)
group by
    passenger_count;
```

**Answer:**

`2: 1282 ; 3: 254`

## Question 6. Largest tip

**Question:**

>For the passengers picked up in the Astoria Zone which was the drop off zone that had the largest tip?
>We want the name of the zone, not the id.
>
>Note: it's not a typo, it's `tip` , not `trip`

**Solution:**

```postgres-sql
select 
    zd."Zone" as dropoff_location_name
from 
    green_taxi_trips gt
    join zones zp on gt."PULocationID" = zp."LocationID"
    join zones zd on gt."DOLocationID" = zd."LocationID"
where
    zp."Zone" = 'Astoria'
order by
    gt.tip_amount desc    
limit
    1;
```

**Answer:**

`Long Island City/Queens Plaza`
