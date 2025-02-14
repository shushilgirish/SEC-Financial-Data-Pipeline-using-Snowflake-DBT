from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import json
from zipfile import ZipFile

# Configuration
AWS_CONN_ID = 'aws_default'
BUCKET_NAME = 'bigdatateam5-pdfreader'
S3_RAW_FOLDER = "sec_raw_data"
S3_JSON_FOLDER = "sec_json_data"
LOCAL_TEMP_FOLDER = "/opt/airflow/temp_data" 
 # Mounted Airflow temp folder
snowflake_schema_json_data = "json_schema"


# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'txt_to_json',
    default_args=default_args,
    description='Convert SEC Financial Statement TXT files to JSON and upload to S3',
    schedule_interval='@daily',
    catchup=False
)

# def download_and_extract_files(**context):
#     """Download SEC zip files from S3, extract TXT files, and process them."""
#     s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    
#     # List all available zip files
#     zip_files = s3_hook.list_keys(bucket_name=BUCKET_NAME, prefix=S3_RAW_FOLDER)
#     if not zip_files:
#         print("⚠️ No ZIP files found in S3.")
#         return

#     for zip_file in zip_files:
#         file_name = zip_file.split('/')[-1]
#         local_zip_path = os.path.join(LOCAL_TEMP_FOLDER, file_name)

#         # Download the zip file
#         s3_hook.get_key(zip_file, bucket_name=BUCKET_NAME).download_file(local_zip_path)
#         print(f"✅ Downloaded {file_name}")

#         # Extract files
#         with ZipFile(local_zip_path, 'r') as zip_ref:
#             zip_ref.extractall(LOCAL_TEMP_FOLDER)

def transform_to_json(**context):
    """Convert extracted TXT files into JSON format."""
    txt_files = ["num.txt", "pre.txt", "sub.txt", "tag.txt"]
    json_data = {}

    for txt_file in txt_files:
        file_path = os.path.join(LOCAL_TEMP_FOLDER, txt_file)
        if os.path.exists(file_path):
            df = pd.read_table(file_path, delimiter="\t", low_memory=False)
            json_data[txt_file.replace(".txt", "")] = df.to_dict(orient="records")

    # Save JSON to temp folder
    json_output_path = os.path.join(LOCAL_TEMP_FOLDER, "sec_financials.json")
    with open(json_output_path, "w") as json_file:
        json.dump(json_data, json_file, indent=4)

    print("✅ JSON transformation complete.")
    context['task_instance'].xcom_push(key='json_output_path', value=json_output_path)

def upload_json_to_s3(**context):
    """Upload the transformed JSON file to S3."""
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    json_output_path = context['task_instance'].xcom_pull(task_ids='transform_to_json', key='json_output_path')
    
    if json_output_path:
        s3_key = f"{S3_JSON_FOLDER}/sec_financials.json"
        s3_hook.load_file(filename=json_output_path, key=s3_key, bucket_name=BUCKET_NAME, replace=True)
        print(f"✅ Uploaded JSON to S3 at {s3_key}")

def stage_json_in_snowflake():
    """Stage JSON files in Snowflake."""
    return SnowflakeOperator(
        task_id='stage_json_in_snowflake',
        snowflake_conn_id='snowflake_default',
        sql=f"""
        use schema json_schema;
        CREATE OR REPLACE STAGE json_stage
        URL='s3://{BUCKET_NAME}/{S3_JSON_FOLDER}/'
        FILE_FORMAT = (TYPE = JSON);
        """,
        dag=dag
    )

def load_json_to_snowflake():
    """Load JSON from S3 into Snowflake JSON_SCHEMA."""
    return SnowflakeOperator(
        task_id='load_json_to_snowflake',
        snowflake_conn_id='snowflake_default',
        sql=f"""
        use schema json_schema;
        COPY INTO {snowflake_schema_json_data}.SEC_JSON_TABLE
        FROM @s3_stage/sec_json_data/sec_financials.json
        FILE_FORMAT = (TYPE = JSON)
        ON_ERROR = 'CONTINUE';
        """,
        dag=dag
    )

# Define tasks
# download_task = PythonOperator(
#     task_id='download_and_extract_files',
#     python_callable=download_and_extract_files,
#     dag=dag
# )

transform_task = PythonOperator(
    task_id='transform_to_json',
    python_callable=transform_to_json,
    provide_context=True,
    dag=dag
)

upload_task = PythonOperator(
    task_id='upload_json_to_s3',
    python_callable=upload_json_to_s3,
    provide_context=True,
    dag=dag
)
stage_task = stage_json_in_snowflake()
load_task = load_json_to_snowflake()


# Set task dependencies
transform_task >> upload_task >> stage_task >>load_task
