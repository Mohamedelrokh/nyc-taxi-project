import json
import psycopg2
from confluent_kafka import Consumer, KafkaError
from datetime import datetime

def read_config():
    """Reads Kafka client configuration from client.properties."""
    config = {}
    with open("ccloud-python-client/client.properties") as fh:
        for line in fh:
            line = line.strip()
            if line and not line.startswith("#"):
                parameter, value = line.split("=", 1)
                config[parameter.strip()] = value.strip()
    config.setdefault("group.id", "python-consumer-group")
    config.setdefault("auto.offset.reset", "earliest")
    return config

def connect_to_db():
    """Connects to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="postgres",
            host="localhost",  # Update if using a remote PostgreSQL instance
            port="5432"
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def insert_taxi_trip(cursor, message_key, tpep_pickup_datetime, tpep_dropoff_datetime, pulocation_id, dolocation_id, passenger_count, trip_distance, fare_amount):
    """Inserts taxi trip data into the PostgreSQL taxi_trips table and associates Kafka key with trip_id."""
    try:
        insert_query = """
            INSERT INTO taxi_trips (message_key, tpep_pickup_datetime, tpep_dropoff_datetime, pulocation_id, dolocation_id, passenger_count, trip_distance,payment_type, fare_amount)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (message_key, tpep_pickup_datetime, tpep_dropoff_datetime, pulocation_id, dolocation_id, passenger_count, trip_distance,payment_type, fare_amount))
        cursor.connection.commit()  # Commit the transaction
        # print(f"Inserted trip data with trip_id: {message_key}")
    except Exception as e:
        print(f"Error inserting trip data: {e}")
def consume_and_insert(topic, config):
    """Consumes messages from Kafka and inserts them into PostgreSQL."""
    consumer = Consumer(config)
    consumer.subscribe([topic])

    conn = connect_to_db()
    if conn is None:
        return

    cursor = conn.cursor()

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
                # Print key, timestamp, partition, and offset
                print(f"Key: {msg.key().decode('utf-8') if msg.key() else None}")
                print(f"Offset: {msg.offset()}")

                # Parse the JSON message from Kafka
                value = json.loads(msg.value().decode("utf-8"))
                
                # Extract the relevant fields from the message
                tpep_pickup_datetime = value["tpep_pickup_datetime"]
                tpep_dropoff_datetime = value["tpep_dropoff_datetime"]
                pulocation_id = value["PULocationID"]
                dolocation_id = value["DOLocationID"]
                passenger_count = value["passenger_count"]
                trip_distance = value["trip_distance"]
                payment_type = value["payment_type"]
                fare_amount = value["fare_amount"]

                # Use the Kafka message key as the trip_id in PostgreSQL
                message_key = msg.key().decode('utf-8')  # Get the key from the Kafka message

                # Insert the data into PostgreSQL with trip_id as the Kafka key
                insert_taxi_trip(cursor, message_key, tpep_pickup_datetime, tpep_dropoff_datetime, pulocation_id, dolocation_id, passenger_count, trip_distance,payment_type, fare_amount)

                # Commit the offset manually after successfully processing the message
                consumer.commit(message=msg)

            except Exception as e:
                print(f"Error processing message: {e}")

    except KeyboardInterrupt:
        print("Interrupted, finishing...")

    finally:
        # Close the cursor and connection when done
        cursor.close()
        conn.close()
        consumer.close()


def main():
    config = read_config()
    topic = "5-try-yellow-trips"  # Kafka topic name (same as the producer's topic)
    consume_and_insert(topic, config)

if __name__ == "__main__":
    main()
