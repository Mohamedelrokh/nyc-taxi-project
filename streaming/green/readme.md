# **Streaming Data Pipeline - Green Taxi Trip Data**

## **Overview**

This project implements a **streaming data pipeline** for processing green taxi trip data using **Kafka**, **PostgreSQL**, and **Power BI** for real-time data visualization. The pipeline consists of three major components:

- **Producer**: Reads Parquet files from a specified directory and sends them to Kafka.
- **Consumer**: Consumes the data from Kafka and inserts it into a PostgreSQL database.
- **Power BI Dashboard**: Visualizes the data in real-time for analysis.



## **Architecture**

The architecture of this data pipeline is composed of the following:

- **Confluent Kafka**: Used as the message broker for streaming data.
- **PostgreSQL**: A relational database used to store the consumed data for further processing and analytics.
- **Power BI**: A dashboard solution that connects to PostgreSQL and visualizes the taxi trip data in real-time.

---

## **Components**

### **1. Producer (`producer.py`)**

The producer reads the green taxi trip data stored as Parquet files and streams each row as a Kafka message. It does so by connecting to the Kafka cluster and producing messages to the Kafka topic `green-trips`.

#### **Key Features**:
- Reads data from Parquet files in the specified directory. (Download the data from [this link](https://d37ci6vzurychx.cloudfront.net/trip-data/))
- Sends each row of the file as a Kafka message to a defined Kafka topic. (Simulate streaming)
- Tracks the progress of file consumption to allow resuming from the last processed point. (To start from where it ended in case of producer script stop)

#### **Requirements**:
- Kafka Confluent details configured in the `client.properties` file.
- `pandas`, `confluent_kafka`, `time`, `json`, and `os` libraries.

#### **Commands to Run the Producer**:
1. **Install Dependencies**:
   ```bash
pip install pandas confluent_kafka
Run the Producer Script: To start streaming data:

python producer.py
This will begin reading Parquet files from the specified directory and stream them into Kafka.

2. Consumer (consumer.py)
The consumer listens to the green-trips Kafka topic, consumes the messages, and inserts the data into a PostgreSQL database. The database table green_taxi_trips stores the taxi trip details such as pickup/drop-off times, location IDs, passenger count, trip distance, payment type, and fare amount.

Key Features:
Connects to a PostgreSQL database and inserts the consumed data into the green_taxi_trips table.

Commits the Kafka offset to ensure message processing continuity.

Handles exceptions and retries for data insertion.

Requirements:
PostgreSQL database connection details.

psycopg2, confluent_kafka, json, and datetime libraries.

Commands to Run the Consumer:
Install Dependencies:

pip install psycopg2-binary confluent_kafka
Run the Consumer Script: To start consuming Kafka messages and inserting them into PostgreSQL:


python consumer.py
The consumer will continuously pull data from Kafka and insert it into PostgreSQL.

3. PostgreSQL Database (Dockerized)
A PostgreSQL instance is containerized using Docker to provide a local environment for storing the taxi trip data. The database is configured to accept connections from the producer and consumer services.

Docker Configuration:
The docker-compose.yml file starts a PostgreSQL container.

The database is exposed on port 5432 for communication between the consumer and producer.

The table green_taxi_trips stores the data from Kafka.

Commands to Run the PostgreSQL Database:
Start Dockerized PostgreSQL:

First, ensure you have Docker and Docker Compose installed.

Create a docker-compose.yml file for the PostgreSQL container:


docker-compose up -d


Create the green_taxi_trips Table: After your PostgreSQL container is running, create the required table for storing taxi trip data:

sql
Copy code
CREATE TABLE green_taxi_trips (
    message_key VARCHAR PRIMARY KEY,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    pulocation_id INT,
    dolocation_id INT,
    passenger_count INT,
    trip_distance FLOAT,
    payment_type VARCHAR,
    fare_amount FLOAT
);
Check the Database: You can now check if the data is being inserted correctly by querying the green_taxi_trips table:

sql
Copy code
SELECT * FROM green_taxi_trips LIMIT 10;
4. Power BI Dashboard
The Power BI dashboard connects to the PostgreSQL database to visualize the green taxi trip data in real-time. It will display various metrics, such as:

Number of trips over time

Trip distances

Fare amounts

Pickup and drop-off locations

Requirements:
Power BI Desktop or Power BI Service.

A PostgreSQL connection configured within Power BI to pull the latest data.

Setup Instructions
1. Environment Setup
Kafka Cluster Setup:

Set up your Kafka cluster (either self-hosted or using Confluent Cloud).

Update client.properties file with the correct Kafka cluster configuration.

PostgreSQL Database:

Set up a PostgreSQL instance. You can use Docker to run PostgreSQL locally:

bash
Copy code
docker-compose up -d
Install Dependencies:

Install the required Python libraries for both producer and consumer:

bash
Copy code
pip install pandas confluent_kafka psycopg2
Create PostgreSQL Table:

Ensure the green_taxi_trips table exists in PostgreSQL:

sql
Copy code
CREATE TABLE green_taxi_trips (
    message_key VARCHAR PRIMARY KEY,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    pulocation_id INT,
    dolocation_id INT,
    passenger_count INT,
    trip_distance FLOAT,
    payment_type VARCHAR,
    fare_amount FLOAT
);
Power BI Setup:

Connect Power BI to your PostgreSQL database.

Create a real-time dashboard using the relevant metrics.

2. Running the Pipeline
Producer: Run the producer to stream the data from the Parquet files into Kafka:

bash
Copy code
python producer.py
Consumer: Run the consumer to consume messages from Kafka and insert them into PostgreSQL:

bash
Copy code
python consumer.py
Power BI: Use Power BI to create visualizations by connecting to the PostgreSQL database.

Configuration Files
client.properties (Kafka Client Configuration)
This file contains the necessary configuration for Kafka connections.

properties
Copy code
bootstrap.servers=<KAFKA_BROKER>
security.protocol=PLAINTEXT
sasl.mechanism=PLAIN
last_produced_state.json
This file tracks the last processed row and file in the producer script.

json
Copy code
{
  "file": "sample_file.parquet",
  "row": 120
}
Future Enhancements
Scaling: Consider using Kafka consumer groups to scale horizontally for higher throughput.

Data Transformation: Implement any necessary transformations on the data before inserting it into PostgreSQL.

Error Handling: Enhance error handling and logging for production-grade reliability.

Automated Monitoring: Set up monitoring on Kafka and PostgreSQL for production environments.

Conclusion
This streaming data pipeline allows real-time processing of green taxi trip data from Parquet files to Kafka, then to PostgreSQL, and finally into Power BI for real-time data visualization. This setup is ideal for gaining insights into taxi operations, passenger trends, and other valuable analytics.

markdown
Copy code

### Key Updates:
- Added **commands** to run the producer, consumer, and PostgreSQL container.
- Included instructions on how to **set up PostgreSQL** using Docker, including creating the 
