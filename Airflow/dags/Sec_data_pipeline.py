import os
import time
import zipfile
import random
import requests
import pandas as pd
import os
import re
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import json


from datetime import datetime, timedelta

# =========================
# SELENIUM IMPORTS
# =========================
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options

# Load configuration from JSON file
with open('/opt/airflow/config/sec_config.json') as config_file:
    config = json.load(config_file)

# S3 Configuration
# Fetch AWS credentials from Airflow connection
AWS_CONN_ID = config['AWS_CONN_ID']
aws_creds = BaseHook.get_connection(AWS_CONN_ID)
BUCKET_NAME = config['BUCKET_NAME']
AWS_ACCESS_KEY = aws_creds.login  # AWS Key
AWS_SECRET_KEY = aws_creds.password  # AWS Secret
S3_BASE_FOLDER = config['S3_BASE_FOLDER']

TEMP_DATA_FOLDER = config['TEMP_DATA_FOLDER']
BASE_URL = config['BASE_URL']
USER_AGENTS = config['USER_AGENTS']
# Snowflake Configuration
SNOWFLAKE_CONN_ID = config['SNOWFLAKE_CONN_ID']
snowflake_conn = BaseHook.get_connection(SNOWFLAKE_CONN_ID)
snowflake_account = snowflake_conn.extra_dejson.get('account')
snowflake_role = snowflake_conn.extra_dejson.get('role')
snowflake_warehouse = snowflake_conn.extra_dejson.get('warehouse')
snowflake_database = snowflake_conn.extra_dejson.get('database')
snowflake_schema_raw_data = snowflake_conn.extra_dejson.get('schema')
# =========================
# DAG DEFAULT ARGS
# =========================
default_args = {
    "owner": config['default_args']['owner'],
    "depends_on_past": config['default_args']['depends_on_past'],
    "start_date": datetime.fromisoformat(config['default_args']['start_date']),
    "retries": config['default_args']['retries'],
    "retry_delay": timedelta(minutes=int(config['default_args']['retry_delay'].split(':')[1]))
}
# =========================
# CONSTANTS / CONFIG
# =========================
DOWNLOAD_FOLDER = os.path.join(TEMP_DATA_FOLDER, "downloads")
EXTRACTED_FOLDER = os.path.join(TEMP_DATA_FOLDER, "extracted")

dag = DAG(
    "selenium_sec_pipeline",
    default_args=default_args,
    description="Use Selenium to scrape SEC data, download, extract, and upload to S3",
    schedule_interval='@daily',  # Change to @daily if needed
    catchup=False,
)

# =========================
# HELPER FUNCTIONS
# =========================

def wait_for_downloads(download_folder, timeout=60):
    """
    Wait until downloads are complete (no *.crdownload files) or until timeout (seconds).
    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        if any(f.endswith(".crdownload") for f in os.listdir(download_folder)):
            time.sleep(2)  # still downloading
        else:
            print("✅ All downloads completed.")
            return True
    print("❌ Timeout: downloads did not complete.")
    return False

# =========================
# MAIN AIRFLOW TASK
# =========================
def main_task(**context):
    """
    Single main task that:
    1) Reads year/quarter from DAG run config or defaults
    2) Uses Selenium to find the needed ZIP link on SEC
    3) Clicks link to download
    4) Extracts files from ZIP
    """
    year_quarter = context["dag_run"].conf.get("year_quarter")

    if not year_quarter:
        raise ValueError("❌ No year_quarter received from Streamlit!")

    required_zip = f"{year_quarter}.zip"
    print(f"🔍 Required ZIP file: {required_zip}")
    os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
    os.makedirs(EXTRACTED_FOLDER, exist_ok=True)

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    # Setup automatic downloads
    prefs = {
        "download.default_directory": DOWNLOAD_FOLDER,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    chrome_options.add_experimental_option("prefs", prefs)
    # Add random user-agent
    chrome_options.add_argument(f"user-agent={random.choice(USER_AGENTS)}")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    # =======================
    # 3) Scrape .zip link
    # =======================
    driver.get(BASE_URL)

    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_all_elements_located((By.XPATH, "//a[contains(@href, '.zip')]"))
        )
        zip_links = driver.find_elements(By.XPATH, "//a[contains(@href, '.zip')]")
        if not zip_links:
            raise AirflowFailException("❌ No .zip links found on the page.")
    except Exception as e:
        driver.quit()
        raise AirflowFailException(f"❌ Error during scraping ZIP links: {str(e)}")

    # Filter only the needed quarter's .zip
    matching_links = [elem for elem in zip_links if required_zip in elem.get_attribute("href")]
    if not matching_links:
        driver.quit()
        raise AirflowFailException(f"❌ No ZIP file found for {year_quarter}.")

    print(f"✅ Found {len(matching_links)} matching link(s) for {required_zip}.")

    # =======================
    # 4) Download the .zip
    # =======================
    for link_elem in matching_links:
        link_url = link_elem.get_attribute("href")
        print(f"⬇️ Starting download for: {link_url}")
        link_elem.click()
        time.sleep(2)  # Let the download begin

    # Wait for all downloads to finish
    download_success = wait_for_downloads(DOWNLOAD_FOLDER, timeout=60)
    driver.quit()

    if not download_success:
        raise AirflowFailException("❌ Downloads did not complete in time.")

    # =======================
    # 5) Extract All Zips
    # =======================
    extracted_folders = []
    for file_name in os.listdir(DOWNLOAD_FOLDER):
        if file_name.endswith(".zip"):
            zip_path = os.path.join(DOWNLOAD_FOLDER, file_name)
            extract_path = os.path.join(EXTRACTED_FOLDER, file_name.replace(".zip", ""))
            os.makedirs(extract_path, exist_ok=True)
            try:
                with zipfile.ZipFile(zip_path, "r") as zip_ref:
                    zip_ref.extractall(extract_path)
                print(f"📂 Extracted: {file_name} => {extract_path}")
                extracted_folders.append(extract_path)
            except zipfile.BadZipFile:
                print(f"❌ Corrupt ZIP file: {file_name}")
            # Remove the downloaded ZIP after extraction
            os.remove(zip_path)

    # Extract the year_quarter part from the folder paths
    year_quarters = [os.path.basename(folder) for folder in extracted_folders]

    # Push the extracted folder paths and year_quarters to XCom
    context['task_instance'].xcom_push(key='extracted_folders', value=extracted_folders)
    context['task_instance'].xcom_push(key='year_quarters', value=year_quarters)

def upload_and_cleanup(**context):
    """Uploads all tab-delimited .txt files from temp_data/YYYYqQ folders to S3 and deletes them after upload."""
    try:
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        uploaded_files = []
        uploaded_folders = []

        # Pull the extracted folder paths and year_quarters from XCom
        extracted_folders = context['task_instance'].xcom_pull(task_ids='selenium_scrape_download_extract_upload', key='extracted_folders')
        year_quarters = context['task_instance'].xcom_pull(task_ids='selenium_scrape_download_extract_upload', key='year_quarters')

        if not extracted_folders:
            print("⚠️ No extracted folders found for upload.")
            return

        for folder, year_quarter in zip(extracted_folders, year_quarters):
            local_folder_path = folder
            s3_folder = f"{S3_BASE_FOLDER}/{year_quarter}"

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

    except Exception as e:
        print(f"❌ Error during upload: {str(e)}")
        raise

# Test Snowflake connection
test_conn = SnowflakeOperator(
    task_id='test_snowflake_connection',
    snowflake_conn_id=f"{SNOWFLAKE_CONN_ID}",
    sql="SELECT CURRENT_TIMESTAMP;",
    dag=dag
)

# Create stage
create_stage = SnowflakeOperator(
    task_id='create_s3_stage',
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql="""
    USE ROLE {{ params.snowflake_role }};
    USE SCHEMA {{ params.snowflake_schema }};

    CREATE or replace STAGE tsv_s3_stage
    URL='s3://{{ params.bucket_name }}/{{ params.s3_base_folder }}/{{ ti.xcom_pull(task_ids='selenium_scrape_download_extract_upload', key='year_quarters')[0] }}/'
    CREDENTIALS=(AWS_KEY_ID='{{ params.aws_access_key }}', AWS_SECRET_KEY='{{ params.aws_secret_key }}');
    """,
    params={
        "snowflake_role": snowflake_role,
        "snowflake_schema": snowflake_schema_raw_data,
        "bucket_name": BUCKET_NAME,
        "s3_base_folder": S3_BASE_FOLDER,
        "aws_access_key": AWS_ACCESS_KEY,
        "aws_secret_key": AWS_SECRET_KEY,
    },
    dag=dag
)

# Create tables for each TSV file
create_tables = SnowflakeOperator(
    task_id='create_snowflake_tables',
    snowflake_conn_id=f"{SNOWFLAKE_CONN_ID}",
    sql=f"""
   -- Table: SUB (Submissions)
    use role {snowflake_role};
    use schema {snowflake_schema_raw_data};
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
    snowflake_conn_id=f"{SNOWFLAKE_CONN_ID}",
    sql=f"""
    use role {snowflake_role};

    use schema {snowflake_schema_raw_data};
    CREATE OR REPLACE FILE FORMAT tsv_format
    TYPE = CSV
    FIELD_DELIMITER = '\t'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    DATE_FORMAT = 'YYYYMMDD'
    EMPTY_FIELD_AS_NULL = TRUE;

    
    """,

    dag=dag
)




# Single operator for entire process
main_operator = PythonOperator(
    task_id="selenium_scrape_download_extract_upload",
    python_callable=main_task,
    dag=dag,
)
upload_task = PythonOperator(
    task_id='upload_tsv_files_and_cleanup',
    python_callable=upload_and_cleanup,
    provide_context=True,
    dag=dag
)
	

# Load data into Snowflake tables
load_to_snowflake = SnowflakeOperator(
    task_id='load_to_snowflake',
    snowflake_conn_id=SNOWFLAKE_CONN_ID,  # Use direct reference, no f-string needed
    sql="""
    USE ROLE {{ params.snowflake_role }};
    USE SCHEMA {{ params.snowflake_schema }};

    COPY INTO {{ params.snowflake_schema }}.RAW_SUB
    FROM @tsv_s3_stage
    PATTERN = '.*sub\.txt'
    FILE_FORMAT = tsv_format
    ON_ERROR = 'CONTINUE';

    COPY INTO {{ params.snowflake_schema }}.RAW_NUM
    FROM @tsv_s3_stage
    PATTERN = '.*num\.txt'
    FILE_FORMAT = tsv_format
    ON_ERROR = 'CONTINUE';

    COPY INTO {{ params.snowflake_schema }}.RAW_PRE
    FROM @tsv_s3_stage
    PATTERN = '.*pre\.txt'
    FILE_FORMAT = tsv_format
    ON_ERROR = 'CONTINUE';

    COPY INTO {{ params.snowflake_schema }}.RAW_TAG
    FROM @tsv_s3_stage
    PATTERN = '.*tag\.txt'
    FILE_FORMAT = tsv_format
    ON_ERROR = 'CONTINUE';
    """,
    params={
        "snowflake_role": snowflake_role,
        "snowflake_schema": snowflake_schema_raw_data,
    },
    dag=dag
)

# Set task dependencies
main_operator >> upload_task >> test_conn >> create_tables >> create_file_format >> create_stage >> load_to_snowflake