import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from google.cloud import bigquery
import pyarrow.parquet as pq
import pyarrow as pa
import gcsfs
import pandas as pd
import time

PROJECT_ID = "di-malrokh-sandbox-malrokh"
BUCKET_NAME = "green-trips"
REGION = "us-central1"

pipeline_options = PipelineOptions()
google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
google_cloud_options.project = PROJECT_ID
google_cloud_options.region = REGION
google_cloud_options.staging_location = f"gs://{BUCKET_NAME}/staging/"
google_cloud_options.temp_location = f"gs://{BUCKET_NAME}/temp/"
pipeline_options.view_as(GoogleCloudOptions).job_name = "merge-partition-parquet"

# File Locations
INPUT_FILE_PATTERN = f"gs://{BUCKET_NAME}/taxi_data/*.parquet"
MERGED_FILE_PATH = f"gs://{BUCKET_NAME}/merged_data/merged.parquet"
PARTITIONED_OUTPUT_DIR = f"gs://{BUCKET_NAME}/processed/"
ERROR_DIR = f"gs://{BUCKET_NAME}/error/"

# BigQuery Table
BQ_RAW_TABLE = "di-malrokh-sandbox-malrokh.taxi_data.raw_trips"

fs = gcsfs.GCSFileSystem()


def insert_into_bigquery(df, table_id):
    """Uploads the DataFrame to BigQuery (appends new rows)."""
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

    # Convert datetime columns
    df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
    df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Wait for the job to complete
    print(f"🚀 Data inserted into BigQuery table {table_id}")


def merge_parquet_files(input_pattern, output_path):
    """Merges multiple Parquet files into a single Parquet file while ensuring schema consistency."""
    all_files = fs.glob(input_pattern)
    if not all_files:
        print("❌ No Parquet files found!")
        return
    
    tables = []
    for file in all_files:
        try:
            with fs.open(file, "rb") as f:
                table = pq.read_table(f)
                tables.append(table)
                fs.rm(file)  # Remove processed files
                
        except Exception as e:
            print(f"⚠️ Skipping invalid file {file} | Error: {e}")
            error_dest = f"{ERROR_DIR}{file.split('/')[-1]}"
            fs.copy(file, error_dest)
            fs.rm(file)
            print(f"🚨 Moved corrupted file to {error_dest}")

    if tables:
        merged_table = pa.concat_tables(tables)
        df = merged_table.to_pandas()

        # Save merged file
        with fs.open(output_path, "wb") as f:
            pq.write_table(merged_table, f)

        print(f"✅ Merged {len(tables)} files into {output_path}")

        # Insert into BigQuery raw table
        insert_into_bigquery(df, BQ_RAW_TABLE)

    else:
        print("❌ No valid files to merge!")


def partition_data_by_datetime(input_parquet_path, output_dir):
    """Reads the merged Parquet file, adds datetime partitions, and writes partitioned files."""
    
    try:
        with fs.open(input_parquet_path, "rb") as f:
            table = pq.read_table(f)
            df = table.to_pandas()
    except Exception as e:
        print(f"❌ Error reading merged Parquet file: {e}")
        return

    # Convert datetime columns
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])

    # Extract year, month, day, hour
    df['pickup_year'] = df['lpep_pickup_datetime'].dt.year
    df['pickup_month'] = df['lpep_pickup_datetime'].dt.month
    df['pickup_day'] = df['lpep_pickup_datetime'].dt.day
    df['pickup_hour'] = df['lpep_pickup_datetime'].dt.hour
    timestamp = time.strftime("%Y%m%d%H%M%S")

    # Partition the data and save to GCS
    for (year, month, day, hour), group in df.groupby(['pickup_year', 'pickup_month', 'pickup_day', 'pickup_hour']):
        partition_path = f"{output_dir}/pickup_year={year}/pickup_month={month}/pickup_day={day}/pickup_hour={hour}/data_{timestamp}.parquet"
        table = pa.Table.from_pandas(group)
        
        with fs.open(partition_path, "wb") as f:
            pq.write_table(table, f)
        
        print(f"✅ Wrote partitioned data to {partition_path}")


# Run merging function first
merge_parquet_files(INPUT_FILE_PATTERN, MERGED_FILE_PATH)

# Run partitioning function after merging
partition_data_by_datetime(MERGED_FILE_PATH, PARTITIONED_OUTPUT_DIR)
