bq mk --transfer_config \
    --project_id=di-malrokh-sandbox-malrokh \
    --transfer_config_id=my_scheduled_query \
    --data_source=scheduled_query \
    --destination_dataset_id=taxi_data \
    --display_name="Insert Raw Data to Partitioned Table Every 6 Hours" \
    --schedule="every 6 hours" \
    --query="
        INSERT INTO \`di-malrokh-sandbox-malrokh.taxi_data.partitioned_trips\` (
            VendorID, lpep_pickup_datetime, lpep_dropoff_datetime, store_and_fwd_flag, RatecodeID, 
            PULocationID, DOLocationID, passenger_count, trip_distance, fare_amount, extra, mta_tax, 
            tip_amount, tolls_amount, ehail_fee, improvement_surcharge, total_amount, payment_type, 
            trip_type, congestion_surcharge, __index_level_0__
        )
        SELECT
            VendorID, lpep_pickup_datetime, lpep_dropoff_datetime, store_and_fwd_flag, RatecodeID, 
            PULocationID, DOLocationID, passenger_count, trip_distance, fare_amount, extra, mta_tax, 
            tip_amount, tolls_amount, ehail_fee, improvement_surcharge, total_amount, payment_type, 
            trip_type, congestion_surcharge, __index_level_0__
        FROM \`di-malrokh-sandbox-malrokh.taxi_data.raw_trips\`
        WHERE lpep_pickup_datetime >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
    " \
    --notification_pubsub_topic="projects/di-malrokh-sandbox-malrokh/topics/your_topic" \
    --write_disposition=WRITE_APPEND
