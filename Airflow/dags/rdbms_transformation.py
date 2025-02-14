from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv(dotenv_path='/opt/airflow/.env')

# Access environment variables
snowflake_env_vars = {
    'SNOWFLAKE_ACCOUNT': os.getenv('SNOWFLAKE_ACCOUNT'),
    'SNOWFLAKE_USER': os.getenv('SNOWFLAKE_USER'),
    'SNOWFLAKE_PASSWORD': os.getenv('SNOWFLAKE_PASSWORD'),
    'SNOWFLAKE_ROLE': os.getenv('SNOWFLAKE_ROLE'),
    'SNOWFLAKE_WAREHOUSE': os.getenv('SNOWFLAKE_WAREHOUSE'),
    'SNOWFLAKE_DATABASE': os.getenv('SNOWFLAKE_DATABASE'),
    'SNOWFLAKE_SCHEMA': os.getenv('SNOWFLAKE_SCHEMA')
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 14),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'rdmbs_dbt_transformation',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

# Define the path to your DBT project
DBT_PROJECT_DIR = "/opt/airflow/masterfindata"

# Define the full path to the dbt executable
DBT_EXECUTABLE = "/home/airflow/.local/bin/dbt"  # Update this path based on the output of `which dbt`

dbt_env = {**snowflake_env_vars}

# Run `dbt debug` to test the connection
dbt_debug = BashOperator(
    task_id='dbt_debug',
    bash_command=f"cd {DBT_PROJECT_DIR}  && {DBT_EXECUTABLE} debug",
    env=dbt_env,
    dag=dag,
)

# Run `dbt run` to execute models
dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command=f"cd {DBT_PROJECT_DIR}  && {DBT_EXECUTABLE} run",
    env=dbt_env,
    dag=dag,
)


# DAG Execution Order
dbt_debug >> dbt_run
