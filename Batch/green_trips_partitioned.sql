CREATE  TABLE `di-malrokh-sandbox-malrokh.taxi_data.green_trips_partitioned` (
    VendorID INT64,
    lpep_pickup_datetime TIMESTAMP,
    lpep_dropoff_datetime TIMESTAMP,
    store_and_fwd_flag STRING,
    RatecodeID FLOAT64,
    PULocationID INT64,
    DOLocationID INT64,
    passenger_count FLOAT64,
    trip_distance FLOAT64,
    fare_amount FLOAT64,
    extra FLOAT64,
    mta_tax FLOAT64,
    tip_amount FLOAT64,
    tolls_amount FLOAT64,
    ehail_fee INT64,
    improvement_surcharge FLOAT64,
    total_amount FLOAT64,
    payment_type FLOAT64,
    trip_type FLOAT64,
    congestion_surcharge FLOAT64,
    __index_level_0__ STRING,  -- Assuming this is an index or other metadata column
    pickup_year INT64,
    pickup_month INT64,
    pickup_day INT64,
    pickup_hour INT64,
    dropoff_year INT64,
    dropoff_month INT64,
    dropoff_day INT64,
    dropoff_hour INT64
)
PARTITION BY DATE(lpep_pickup_datetime)  -- Partition by pickup_datetime field (Day-level partitioning)
 CLUSTER BY   PULocationID   ;  -- Cluster by VendorID or another column that helps with query performance
