# NYC Taxi Project Stream & Batch Pipeline

![Project Image](https://drive.google.com/uc?id=1Fop7D8tE_2gDahJX5TFjXyf2RULH5ndN)




## Overview
This project processes NYC taxi data using both **batch** and **streaming** pipelines. The architecture leverages Google Cloud, Kafka, PostgreSQL, and various data processing tools to ensure efficient data ingestion, transformation, and visualization.

## Architecture
The system consists of multiple components that work together to collect, process, and analyze taxi trip data from NYC OpenData. Below is a breakdown of the two pipelines:

### **1. Streaming Pipeline**
The streaming pipeline ensures real-time processing of NYC taxi trip data.

#### **Workflow:**
1. **Raw Data Source:**
   - NYC OpenData provides real-time taxi trip data.
   - Python scripts (`producer.py`) push data -in milliseconds to simulate streaming- into Kafka topics (Green Taxi & Yellow Taxi).
2. **Ingestion:**
   - **Confluent Kafka** acts as the messaging system to receive and distribute data.
   - Data is pushed into separate topics (`Green Topic` and `Yellow Topic`).
3. **Processing & Storage:**
   - Consumers process messages and store them in **PostgreSQL (Docker-based instance)**.
   - Data is also written into **Google Cloud Storage** (raw zone) and **BigQuery** (data warehouse for analytics).
4. **Analytics & Visualization:**
   - **dbt** (Data Build Tool) is used for data transformation and modeling.
   - Data is visualized using **Power BI** via direct queries on PostgreSQL (for real-time insights).

### **2. Batch Pipeline**
The batch pipeline processes historical data at scheduled intervals.

#### **Workflow:**
1. **Raw Data Source:**
   - Other consumers collects 100 messages from kafka topics and store them  in **Google Cloud Storage (Raw Zone).** as **parquet** files.
2. **Processing & Transformation:**
   - **Google Dataflow** processes raw data and loads it into **BigQuery**.
   - Data moves through the **Trusted Zone** and is refined in **Refined Zone**.
   - dbt transformations are applied for advanced analytics.
3. **Scheduled Analytics & Visualization:**
   - Preprocessed data is imported into **Power BI** for scheduled reports and dashboards.
   - Dashboards provide business insights into taxi ride trends.

## **Tools, Platforms & Services Used**

### **Cloud & Compute Services:**
- **Google Cloud Platform (GCP):** Provides scalable cloud storage, BigQuery, and orchestration.
- **Docker:** Used to containerize the PostgreSQL database.
- **Terraform:** Infrastructure-as-Code (IaC) to provision cloud resources.

### **Data Ingestion & Processing:**
- **Confluent Kafka:** Message queue for real-time streaming ingestion.
- **Google Dataflow:** Batch processing and ETL for BigQuery.
- **Cloud Composer:** Workflow orchestration for data pipelines.

### **Storage & Database:**
- **Google Cloud Storage:** Stores raw data before processing.
- **PostgreSQL (Docker):** Stores real-time processed data.
- **BigQuery:** Serves as a data warehouse for historical analysis.

### **Analytics & Visualization:**
- **dbt:** Data transformation and modeling.
- **Power BI:** Business Intelligence tool for reports and dashboards.
- **GitHub:** Version control for dbt and pipeline scripts.

## **Conclusion**
This project demonstrates an efficient, scalable data pipeline for NYC taxi trip data using both real-time and batch processing methods. It integrates cloud-based storage, data transformation, and visualization tools to generate meaningful insights.


