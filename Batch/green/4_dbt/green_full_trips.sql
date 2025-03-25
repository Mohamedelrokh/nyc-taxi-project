WITH aggregated_data AS (
    SELECT * FROM {{ ref('green_grouped_trips') }}  -- Reference the aggregation model
)

SELECT
    a.*,
    pu.Zone AS pickup_zone,
    pu.Borough AS pickup_borough,
    pu.service_zone AS pickup_service_zone ,
    pu.latitude AS pickup_latitute ,
    pu.longitude AS pickup_longitude ,
    do.Zone AS dropoff_zone,
    do.Borough AS dropoff_borough,
    do.service_zone AS dropoff_service_zone,
    do.latitude AS dropoff_latitute ,
    do.longitude AS dropoff_longitude ,
FROM
    aggregated_data a
LEFT JOIN
    {{ ref('taxi_zones_coordinates') }} pu
    ON a.PULocationID = pu.LocationID
LEFT JOIN
    {{ ref('taxi_zones_coordinates') }} do
    ON a.DOLocationID = do.LocationID
ORDER BY
    a.pickup_year, a.pickup_month, a.pickup_day
