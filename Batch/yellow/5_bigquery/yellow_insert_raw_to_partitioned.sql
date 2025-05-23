INSERT INTO `di-malrokh-sandbox-malrokh.taxi_data.yellow_trips_partitioned` (
    VendorID,
    lpep_pickup_datetime,
    lpep_dropoff_datetime,
    store_and_fwd_flag,
    RatecodeID,
    PULocationID,
    DOLocationID,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type,
    trip_type,
    congestion_surcharge,
    pickup_year,
    pickup_month,
    pickup_day,
    pickup_hour,
    dropoff_year,
    dropoff_month,
    dropoff_day,
    dropoff_hour
)
SELECT
    VendorID,
    lpep_pickup_datetime,
    lpep_dropoff_datetime,
    store_and_fwd_flag,
    RatecodeID,
    PULocationID,
    DOLocationID,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type,
    trip_type,
    congestion_surcharge,
    EXTRACT(YEAR FROM lpep_pickup_datetime) AS pickup_year,
    EXTRACT(MONTH FROM lpep_pickup_datetime) AS pickup_month,
    EXTRACT(DAY FROM lpep_pickup_datetime) AS pickup_day,
    EXTRACT(HOUR FROM lpep_pickup_datetime) AS pickup_hour,
    EXTRACT(YEAR FROM lpep_dropoff_datetime) AS dropoff_year,
    EXTRACT(MONTH FROM lpep_dropoff_datetime) AS dropoff_month,
    EXTRACT(DAY FROM lpep_dropoff_datetime) AS dropoff_day,
    EXTRACT(HOUR FROM lpep_dropoff_datetime) AS dropoff_hour
FROM `di-malrokh-sandbox-malrokh.taxi_data.yellow_trips_raw`;  -- Replace with your raw table name













