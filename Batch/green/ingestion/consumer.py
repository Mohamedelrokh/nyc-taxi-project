import json
import os
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError
from google.cloud import storage
from datetime import datetime

# Google Cloud Storage Configuration
BUCKET_NAME = "green-trips"
FOLDER_NAME = "taxi_data"
LOCAL_TMP_DIR = "./tmp"  # Temporary local directory for buffering

# Ensure local directory exists
os.makedirs(LOCAL_TMP_DIR, exist_ok=True)

def read_config():
    """Reads Kafka client configuration from client.properties."""
    config = {}
    with open("../ccloud-python-client/client.properties") as fh:
        for line in fh:
            line = line.strip()
            if line and not line.startswith("#"):
                parameter, value = line.split("=", 1)
                config[parameter.strip()] = value.strip()
    config.setdefault("group.id", "python-consumer-group")
    config.setdefault("auto.offset.reset", "earliest")
    config["enable.auto.commit"] = "false"  # Disable auto commit
    return config

def upload_to_gcs(local_file, destination_blob_name):
    """Uploads a file to Google Cloud Storage."""
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(local_file)
    print(f"Uploaded {local_file} to gs://{BUCKET_NAME}/{destination_blob_name}")
    os.remove(local_file)  # Remove the local file after upload

def consume_and_store(topic, config):
    """Consumes messages from Kafka and writes them to GCS as Parquet files."""
    consumer = Consumer(config)
    consumer.subscribe([topic])

    batch = []
    batch_size = 100  # Number of messages per file
    file_counter = 0

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print("End of partition reached")
                else:
                    print("Error:", msg.error())
                continue

            try:
                value = json.loads(msg.value().decode("utf-8"))
                batch.append(value)
                
                if len(batch) >= batch_size:
                    # Convert batch to a Parquet file
                    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
                    local_file = os.path.join(LOCAL_TMP_DIR, f"taxi_data_{timestamp}_{file_counter}.parquet")

                    # Convert the batch to a pandas DataFrame and then to Parquet
                    import pandas as pd
                    df = pd.DataFrame(batch)
                    table = pa.Table.from_pandas(df)
                    pq.write_table(table, local_file)

                    destination_blob_name = f"{FOLDER_NAME}/taxi_data_{timestamp}_{file_counter}.parquet"
                    upload_to_gcs(local_file, destination_blob_name)

                    batch.clear()
                    file_counter += 1

                # Commit the offset **after** the message is processed and uploaded
                consumer.commit(message=msg)

            except Exception as e:
                print(f"Error processing message: {e}")

    except KeyboardInterrupt:
        print("Interrupted, finishing...")
        if batch:
            timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
            local_file = os.path.join(LOCAL_TMP_DIR, f"taxi_data_{timestamp}_{file_counter}.parquet")

            # Convert the batch to a pandas DataFrame and then to Parquet
            import pandas as pd
            df = pd.DataFrame(batch)
            table = pa.Table.from_pandas(df)
            pq.write_table(table, local_file)

            destination_blob_name = f"{FOLDER_NAME}/taxi_data_{timestamp}_{file_counter}.parquet"
            upload_to_gcs(local_file, destination_blob_name)

    finally:
        consumer.close()

def main():
    config = read_config()
    topic = "green-trips"  # Update with your Kafka topic
    consume_and_store(topic, config)

if __name__ == "__main__":
    main()
