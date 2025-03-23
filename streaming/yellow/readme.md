# **Streaming Data Pipeline - yellow Taxi Trip Data**

## **Overview**

This project implements a **streaming data pipeline** for processing yellow taxi trip data using **Kafka**, **PostgreSQL**, and **Power BI** for real-time data visualization. The pipeline consists of three major components:

- **Producer**: Reads Parquet files from a specified directory and sends them to Kafka.
- **Consumer**: Consumes the data from Kafka and inserts it into a PostgreSQL database.
- **Power BI Dashboard**: Visualizes the data in real-time for analysis.

---

## **Architecture**

The architecture of this data pipeline is composed of the following:

- **Confluent Kafka**: Used as the message broker for streaming data.
- **PostgreSQL**: A relational database used to store the consumed data for further processing and analytics.
- **Power BI**: A dashboard solution that connects to PostgreSQL and visualizes the taxi trip data in real-time with setting DirectQuery to enable real-time updates

---

## **Components**

### **1. Producer (`producer.py`)**

The producer reads the yellow taxi trip data stored as Parquet files and streams each row as a Kafka message. It does so by connecting to the Kafka cluster and producing messages to the Kafka topic `yellow-trips`.

#### **Key Features**:
- Reads data from Parquet files in the specified directory.(download the data from this link https://d37ci6vzurychx.cloudfront.net/trip-data/ )
- Sends each row of the file as a Kafka message to a defined Kafka topic.(simulate streaming)
- Tracks the progress of file consumption to allow resuming from the last processed point.(to start from where it ended in case of producer script  stopped)

#### **Requirements**:
- Kafka confluent  details configured in the `client.properties` file.
- `pandas`, `confluent_kafka`, `time`, `json`, and `os` libraries.

---

### **2. Consumer (`consumer.py`)**

The consumer listens to the `yellow-trips` Kafka topic, consumes the messages, and inserts the data into a PostgreSQL database. The database table `yellow_taxi_trips` stores the taxi trip details such as pickup/drop-off times, location IDs, passenger count, trip distance, payment type, and fare amount.

#### **Key Features**:
- Connects to a PostgreSQL database and inserts the consumed data into the `yellow_taxi_trips` table.
- Commits the Kafka offset to ensure message processing continuity.
- Handles exceptions and retries for data insertion.

#### **Requirements**:
- PostgreSQL database connection details.
- `psycopg2`, `confluent_kafka`, `json`, and `datetime` libraries.


### **3. PostgreSQL Database (Dockerized)**

A **PostgreSQL instance** is containerized using **Docker** to provide a local environment for storing the taxi trip data. The database is configured to accept connections from the producer and consumer services.

#### **Docker Configuration**:
- The `docker-compose.yml` file starts a PostgreSQL container.
- The database is exposed on port `5432` for communication between the consumer and producer.
- The table `yellow_taxi_trips` stores the data from Kafka. 
