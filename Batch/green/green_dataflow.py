import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
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

INPUT_FILE_PATTERN = f"gs://{BUCKET_NAME}/taxi_data/*.parquet"
MERGED_FILE_PATH = f"gs://{BUCKET_NAME}/merged_data/merged.parquet"
PARTITIONED_OUTPUT_DIR = f"gs://{BUCKET_NAME}/processed/"
ERROR_DIR = f"gs://{BUCKET_NAME}/error/"

fs = gcsfs.GCSFileSystem()


### 1️⃣ Function to merge all Parquet files into a single file ###
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

                # Ensure schema consistency: Convert 'ehail_fee' to float64
                if "ehail_fee" in table.schema.names:
                    table = table.set_column(
                        table.schema.get_field_index("ehail_fee"),
                        "ehail_fee",
                        pa.array(table.column("ehail_fee").to_pandas().astype("float64"), type=pa.float64())
                    )
                else:
                    # Add missing `ehail_fee` column with nulls (float64)
                    null_array = pa.array([None] * len(table), type=pa.float64())
                    table = table.append_column("ehail_fee", null_array)

                tables.append(table)
        except Exception as e:
            print(f"⚠️ Skipping invalid file {file} | Error: {e}")

            # Move the error file to ERROR_DIR
            try:
                error_dest = f"{ERROR_DIR}{file.split('/')[-1]}"
                fs.copy(file, error_dest)
                fs.rm(file)  # Remove the file from the original directory
                print(f"🚨 Copied corrupted file to {error_dest} and removed it from source")
            except Exception as copy_error:
                print(f"❌ Failed to copy {file} to error directory | Error: {copy_error}")

    if tables:
        merged_table = pa.concat_tables(tables, promote=True)
        with fs.open(output_path, "wb") as f:
            pq.write_table(merged_table, f)
        print(f"✅ Merged {len(tables)} files into {output_path}")
    else:
        print("❌ No valid files to merge!")


### 2️⃣ Function to create partitioned Parquet files ###
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
