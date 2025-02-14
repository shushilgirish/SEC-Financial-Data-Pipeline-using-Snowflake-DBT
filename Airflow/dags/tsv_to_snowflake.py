from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import pandas as pd
import os
import re

# S3 Configuration
AWS_CONN_ID = 'aws_default'
BUCKET_NAME = 'bigdatateam5-pdfreader'
LOCAL_ROOT_FOLDER = "/opt/airflow/temp_data"  # Mounted root folder
S3_BASE_FOLDER = "sec_raw_data"
# Define schema as a variable
snowflake_schema_raw_data = "dbt_schema"



# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Create DAG
dag = DAG(
    'tsv_to_snowflake',
    default_args=default_args,
    description='Upload TSV files to S3 and load to Snowflake',
    schedule_interval='@daily',
    catchup=False
)

def upload_and_cleanup(**context):
    """Uploads all tab-delimited .txt files from temp_data/YYYYqQ folders to S3 and deletes them after upload."""
    try:
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        uploaded_files = []
        uploaded_folders = []

        # Get list of all YYYYqQ subfolders inside temp_data/
        all_folders = [
            f for f in os.listdir(LOCAL_ROOT_FOLDER)
            if re.match(r'^\d{4}q[1-4]$', f) and os.path.isdir(os.path.join(LOCAL_ROOT_FOLDER, f))
        ]

        if not all_folders:
            print("⚠️ No valid folders found for upload.")
            return

        for folder in all_folders:
            local_folder_path = os.path.join(LOCAL_ROOT_FOLDER, folder)
            s3_folder = f"{S3_BASE_FOLDER}/{folder}"

            print(f"🚀 Processing folder: {folder}")

            for file_name in os.listdir(local_folder_path):
                if file_name.endswith(".txt"):
                    local_file_path = os.path.join(local_folder_path, file_name)

                    # Upload to S3
                    s3_key = f"{s3_folder}/{file_name}"
                    print(f"Uploading {file_name} to S3 at {s3_key}...")

                    s3_hook.load_file(
                        filename=local_file_path,
                        key=s3_key,
                        bucket_name=BUCKET_NAME,
                        replace=True
                    )

                    uploaded_files.append(s3_key)
                    print(f"✅ Uploaded: {s3_key}")

            # After successful upload, delete the folder
            for file in os.listdir(local_folder_path):
                os.remove(os.path.join(local_folder_path, file))  # Delete files
            os.rmdir(local_folder_path)  # Remove folder
            uploaded_folders.append(folder)
            print(f"🗑️ Deleted folder: {local_folder_path}")

        print("🎉 Upload and cleanup complete for folders:", uploaded_folders)

        # Push uploaded file paths to XCom for the next task
        # context['task_instance'].xcom_push(key='uploaded_files', value=uploaded_files)

    except Exception as e:
        print(f"❌ Error during upload: {str(e)}")
        raise

# Test Snowflake connection
test_conn = SnowflakeOperator(
    task_id='test_snowflake_connection',
    snowflake_conn_id='snowflake_default',
    sql="SELECT CURRENT_TIMESTAMP;",
    dag=dag
)

# Create stage
create_stage = SnowflakeOperator(
    task_id='create_s3_stage',
    snowflake_conn_id='snowflake_default',
    sql="""
    use role dbt_role;
    use schema dbt_schema;
    CREATE STAGE IF NOT EXISTS tsv_s3_stage
    URL='s3://bigdatateam5-pdfreader/sec_raw_data/'
    CREDENTIALS=(AWS_KEY_ID='{{ conn.aws_default.login }}'
                AWS_SECRET_KEY='{{ conn.aws_default.password }}');
    """,
    dag=dag
)

# Create tables for each TSV file
create_tables = SnowflakeOperator(
    task_id='create_snowflake_tables',
    snowflake_conn_id='snowflake_default',
    sql=f"""
   -- Table: SUB (Submissions)
    use role dbt_role;
    use schema dbt_schema;
CREATE OR REPLACE TABLE {snowflake_schema_raw_data}.RAW_SUB (
    adsh STRING(20), -- Accession Number
    cik NUMBER(10), -- Central Index Key (CIK)
    name STRING(150), -- Registrant Name
    sic NUMBER(4), -- Standard Industrial Classification
    countryba STRING(2), -- Business Address Country
    stprba STRING(2), -- Business Address State/Province
    cityba STRING(30), -- Business Address City
    zipba STRING(10), -- Business Address Zip Code
    bas1 STRING(40), -- Business Address Street Line 1
    bas2 STRING(40), -- Business Address Street Line 2
    baph STRING(20), -- Business Address Phone
    countryma STRING(2), -- Mailing Address Country
    stprma STRING(2), -- Mailing Address State/Province
    cityma STRING(30), -- Mailing Address City
    zipma STRING(10), -- Mailing Address Zip Code
    mas1 STRING(40), -- Mailing Address Street Line 1
    mas2 STRING(40), -- Mailing Address Street Line 2
    countryinc STRING(3), -- Country of Incorporation
    stprinc STRING(2), -- State of Incorporation
    ein NUMBER(10), -- Employer Identification Number
    former STRING(150), -- Most Recent Former Name
    changed STRING(8), -- Date of Change from Former Name
    afs STRING(5), -- Filer Status
    wksi BOOLEAN, -- Well Known Seasoned Issuer (1 or 0)
    fye STRING(4), -- Fiscal Year End Date (mmdd)
    form STRING(10), -- Submission Type
    period DATE, -- Balance Sheet Date
    fy NUMBER(4), -- Fiscal Year Focus
    fp STRING(2), -- Fiscal Period Focus
    filed DATE, -- Filing Date
    accepted TIMESTAMP, -- Acceptance Date & Time
    prevrpt BOOLEAN, -- Previous Report Indicator
    detail BOOLEAN, -- Detail Level Indicator
    instance STRING(40), -- XBRL Instance Document
    nciks NUMBER(4), -- Number of Central Index Keys
    aciks STRING(120) -- Additional Co-Registrants
);

-- Table: TAG (Tags)
CREATE OR REPLACE TABLE {snowflake_schema_raw_data}.RAW_TAG (
    tag STRING(256),
    version STRING(20),
    custom BOOLEAN, -- 1 if custom, 0 if standard
    abstract BOOLEAN, -- 1 if abstract, 0 if numeric
    datatype STRING(20), -- NULL if abstract = 1
    iord STRING(1), -- "I" for point-in-time, "D" for duration
    crdr STRING(1), -- "C" for credit, "D" for debit
    tlabel STRING(512), -- Tag Label
    doc STRING -- Tag Definition
);

-- Table: NUM (Numbers)
CREATE OR REPLACE TABLE {snowflake_schema_raw_data}.RAW_NUM (
    adsh STRING(20), -- Accession Number
    tag STRING(256),
    version STRING(20),
    ddate DATE, -- End Date
    qtrs NUMBER(8), -- Number of Quarters (0 = point-in-time)
    uom STRING(20), -- Unit of Measure
    segments STRING(1024), -- Axis/Member Reporting
    coreg STRING(256), -- Co-Registrant
    value NUMBER(28,4), -- Numeric Value
    footnote STRING(512) -- Footnote
);

-- Table: PRE (Presentation)
CREATE OR REPLACE TABLE {snowflake_schema_raw_data}.RAW_PRE (
    adsh STRING(20), -- Accession Number
    report NUMBER(6), -- Report ID
    line NUMBER(6), -- Line Number
    stmt STRING(2), -- Statement Type (BS, IS, CF, EQ, CI, SI, UN)
    inpth BOOLEAN, -- Presented Parenthetically
    rfile STRING(1), -- Rendered File Type (H = HTML, X = XML)
    tag STRING(256),
    version STRING(20),
    plabel STRING(512), -- Preferred Label
    negating BOOLEAN -- Negation Flag
);

    """,
    dag=dag
)

# Create file format for CSV
create_file_format = SnowflakeOperator(
    task_id='create_file_format',
    snowflake_conn_id='snowflake_default',
    sql="""
    use role dbt_role;

    use schema dbt_schema;
    CREATE OR REPLACE FILE FORMAT tsv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = '\t'  -- TAB delimiter
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE;
    """,
    dag=dag
)

# Upload TSV files task
# Upload TSV files task
upload_task = PythonOperator(
    task_id='upload_tsv_files_and_cleanup',
    python_callable=upload_and_cleanup,
    provide_context=True,
    dag=dag
)
# Load data into Snowflake tables
load_to_snowflake = SnowflakeOperator(
    task_id='load_to_snowflake',
    snowflake_conn_id='snowflake_default',
    sql=f"""
    use role dbt_role;

    use schema dbt_schema;
    COPY INTO {snowflake_schema_raw_data}.RAW_SUB
    FROM @tsv_s3_stage/2024q4/
    PATTERN = '.*sub\.txt',
    FILE_FORMAT = tsv_format,
    ON_ERROR = 'CONTINUE';
    
    COPY INTO {snowflake_schema_raw_data}.RAW_NUM
    FROM @tsv_s3_stage/2024q4/
    PATTERN = '.*num\.txt',
    FILE_FORMAT = tsv_format,
    ON_ERROR = 'CONTINUE';
    
    COPY INTO {snowflake_schema_raw_data}.RAW_PRE
    FROM @tsv_s3_stage/2024q4/
    PATTERN = '.*pre\.txt',
    FILE_FORMAT = tsv_format,
    ON_ERROR = 'CONTINUE';
    
    COPY INTO {snowflake_schema_raw_data}.RAW_TAG
    FROM @tsv_s3_stage/2024q4/
    PATTERN = '.*tag\.txt',
    FILE_FORMAT = tsv_format,
    ON_ERROR = 'CONTINUE';
    """,
    dag=dag
)

# Set task dependencies
# test_conn >> create_stage >> create_file_format >> create_tables >> upload_task >> load_to_snowflake
# Set task dependencies in the new order
test_conn >> create_tables >> create_file_format >> upload_task >> create_stage >> load_to_snowflake

