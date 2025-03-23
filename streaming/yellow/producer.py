import json
import pandas as pd
import time
import os
from confluent_kafka import Producer

# Directories
SAMPLES_DIR = r"C:\Work\Learning\Zoomcamp\project\batch_pipeline\nyc_taxi_data\yellow"  # Folder with sampled files
STATE_FILE = "last_produced_state.json"  # File to track last processed file & row

# Kafka Topic
TOPIC_NAME = "yellow-trips"

def read_config():
    """Reads the Kafka client configuration from client.properties and returns it as a dictionary."""
    config = {}
    with open("../ccloud-python-client/client.properties") as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                config[parameter] = value.strip()
    return config

def get_last_produced_state():
    """Reads the last processed file and row from disk."""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {"file": None, "row": 0}

def save_last_produced_state(file_name, row):
    """Saves the last processed file and row to disk."""
    with open(STATE_FILE, "w") as f:
        json.dump({"file": file_name, "row": row}, f)

def produce_file(file_path, topic, config, start_row=0):
    """Reads a Parquet file and sends its data as messages to Kafka, starting from the last processed row."""
    producer = Producer(config)

    # Read the Parquet file (All columns)
    df = pd.read_parquet(file_path)

    # Convert Timestamp columns to string format (if they exist)
    for col in df.select_dtypes(include=['datetime64']).columns:
        df[col] = df[col].astype(str)

    print(f"Processing file: {file_path} ({len(df)} rows), starting from row {start_row}")

    # Send each row as a Kafka message
    for row_index in range(start_row, len(df)):
        message = df.iloc[row_index].to_dict()
        producer.produce(topic, key=str(row_index), value=json.dumps(message))
        print(f"Produced message {row_index}: {message}")

        save_last_produced_state(os.path.basename(file_path), row_index + 1)  # Save progress after each row

        time.sleep(1)  # Delay between messages

    producer.flush()  # Ensure all messages are sent
    print(f"Finished processing {file_path}")

def main():
    config = read_config()
    
    # Get all sampled files
    files = sorted([f for f in os.listdir(SAMPLES_DIR) if f.endswith(".parquet")])
    
    if not files:
        print("No sampled files found. Exiting...")
        return

    last_state = get_last_produced_state()
    last_produced_file = last_state["file"]
    last_produced_row = last_state["row"]

    # Find the starting file index
    start_index = files.index(last_produced_file) if last_produced_file in files else 0

    # Process files sequentially
    for file_index in range(start_index, len(files)):
        file_name = files[file_index]
        file_path = os.path.join(SAMPLES_DIR, file_name)

        # If it's resuming the same file, start from the last row
        start_row = last_produced_row if file_name == last_produced_file else 0

        produce_file(file_path, TOPIC_NAME, config, start_row)

        # Reset row tracking after finishing a file
        save_last_produced_state(file_name, 0)

    print("All files processed successfully!")

if __name__ == '__main__':
    main()
