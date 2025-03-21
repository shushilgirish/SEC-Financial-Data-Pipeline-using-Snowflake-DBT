from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

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
    'json_dbt_transformation',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

# Define the path to your DBT project
DBT_PROJECT_DIR = "/opt/airflow/json_transform"
# Define the full path to the dbt executable
DBT_EXECUTABLE = "/home/airflow/.local/bin/dbt"

# Define common environment variables for DBT tasks
dbt_env = {**snowflake_env_vars}

# Run `dbt debug` to test the connection
dbt_debug = BashOperator(
    task_id='dbt_debug',
    bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_EXECUTABLE} debug",
    env=dbt_env,
    dag=dag,
)

# Run `dbt run` to execute models
dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_EXECUTABLE} run",
    env=dbt_env,
    dag=dag,
)

# # Run `dbt test` to validate transformations
# dbt_test = BashOperator(
#     task_id='dbt_test',
#     bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_EXECUTABLE} test",
#     env=dbt_env,
#     dag=dag,
# )

# DAG Execution Order
dbt_debug >> dbt_run 
# >> dbt_test