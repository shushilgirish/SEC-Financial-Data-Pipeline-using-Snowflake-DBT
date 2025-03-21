from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(dotenv_path='/opt/airflow/.env')

# Access environment variables for Snowflake connection
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

# Define the path to your DBT project and executable
DBT_PROJECT_DIR = "/opt/airflow/json_transform"
DBT_EXECUTABLE = "/home/airflow/.local/bin/dbt"

# Hard-code DBT_PROFILES_DIR so dbt uses the profiles.yml from the project directory.
# Also, include Snowflake environment variables.
dbt_env = {
    **snowflake_env_vars,
    'DBT_PROFILES_DIR': DBT_PROJECT_DIR
}

# Run `dbt debug --debug` to get detailed output and test the connection
dbt_debug = BashOperator(
    task_id='dbt_debug',
    bash_command=(
        f"cd {DBT_PROJECT_DIR} && "
        f"export DBT_PROFILES_DIR={DBT_PROJECT_DIR} && "
        f"echo 'DBT_PROFILES_DIR is set to:' $DBT_PROFILES_DIR && "
        f"{DBT_EXECUTABLE} debug --debug"
    ),
    env=dbt_env,
    dag=dag,
)

# Run `dbt run` to execute models (without --debug for normal run)
dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command=(
        f"cd {DBT_PROJECT_DIR} && "
        f"export DBT_PROFILES_DIR={DBT_PROJECT_DIR} && "
        f"{DBT_EXECUTABLE} run"
    ),
    env=dbt_env,
    dag=dag,
)

dbt_debug >> dbt_run
