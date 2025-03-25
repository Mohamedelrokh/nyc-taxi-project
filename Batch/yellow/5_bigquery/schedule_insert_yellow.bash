bq mk --transfer_config \
    --project_id=di-malrokh-sandbox-malrokh \
    --transfer_config_id=my_scheduled_query \
    --data_source=scheduled_query \
    --destination_dataset_id=taxi_data \
    --display_name="Insert green Raw Data to Partitioned Table Every 6 Hours" \
    --schedule="every 6 hours" \
    --query="
        INSERT INTO \`di-malrokh-sandbox-malrokh.taxi_data.yellow_trips_partitioned\` (
            VendorID, lpep_pickup_datetime, lpep_dropoff_datetime, store_and_fwd_flag, RatecodeID, 
            PULocationID, DOLocationID, passenger_count, trip_distance, fare_amount, extra, mta_tax, 
            tip_amount, tolls_amount, ehail_fee, improvement_surcharge, total_amount, payment_type, 
            trip_type, congestion_surcharge
        )
        SELECT
            VendorID, lpep_pickup_datetime, lpep_dropoff_datetime, store_and_fwd_flag, RatecodeID, 
            PULocationID, DOLocationID, passenger_count, trip_distance, fare_amount, extra, mta_tax, 
            tip_amount, tolls_amount, ehail_fee, improvement_surcharge, total_amount, payment_type, 
            trip_type, congestion_surcharge
        FROM \`di-malrokh-sandbox-malrokh.taxi_data.yellow_trips_raw\`
        WHERE lpep_pickup_datetime >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
    " \
    --write_disposition=WRITE_APPEND
