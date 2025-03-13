from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import subprocess

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
    'SNOWFLAKE_SCHEMA': os.getenv('SNOWFLAKE_SCHEMA'),
    # Set a higher statement timeout to avoid timeouts
    'SNOWFLAKE_QUERY_TAG': 'AIRFLOW_DBT',
    'SNOWFLAKE_CLIENT_SESSION_KEEP_ALIVE': 'true'
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 14),
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(hours=2),  # Set a longer timeout
}

dag = DAG(
    'rdmbs_dbt_transformation',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1
)

# Define the path to your DBT project
DBT_PROJECT_DIR = "/opt/airflow/masterfindata"
DBT_EXECUTABLE = "/home/airflow/.local/bin/dbt"

dbt_env = {**snowflake_env_vars}

# Run dbt with performance args
def run_dbt_with_performance(models_selector, **kwargs):
    cmd = [
        DBT_EXECUTABLE,
        "run",
        "--models", models_selector,
        "--vars", '{"optimize_performance": true}',
        "--threads", "8",  # Increase thread count for better parallelism
        "--profiles-dir", DBT_PROJECT_DIR
    ]    

    process = subprocess.Popen(
        cmd,
        cwd=DBT_PROJECT_DIR,
        env={**os.environ, **dbt_env},
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )
    
    # Stream output to logs
    for line in process.stdout:
        print(line, end='')
    
    process.wait()
    if process.returncode != 0:
        raise Exception(f"dbt run for {models_selector} failed with error code {process.returncode}")

# Operators with Python execution for more control
dbt_debug = BashOperator(
    task_id='dbt_debug',
    bash_command=f"""
        cd {DBT_PROJECT_DIR}
        echo "DBT Project Dir: $(pwd)"
        echo "Listing directory contents:"
        ls -la
        echo "\nEnvironment variables:"
        echo "SNOWFLAKE_ACCOUNT: $SNOWFLAKE_ACCOUNT"
        echo "SNOWFLAKE_USER: $SNOWFLAKE_USER" 
        echo "SNOWFLAKE_ROLE: $SNOWFLAKE_ROLE"
        echo "SNOWFLAKE_WAREHOUSE: $SNOWFLAKE_WAREHOUSE"
        echo "SNOWFLAKE_DATABASE: $SNOWFLAKE_DATABASE" 
        echo "SNOWFLAKE_SCHEMA: $SNOWFLAKE_SCHEMA"
        echo "\nProfiles.yml content:"
        cat profiles.yml
        echo "\nRunning dbt debug with verbose output:"
        {DBT_EXECUTABLE} debug --profiles-dir {DBT_PROJECT_DIR} -v
    """,
    env=dbt_env,
    dag=dag,
)

dbt_deps = BashOperator(
    task_id='dbt_deps',
    bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_EXECUTABLE} deps --profiles-dir {DBT_PROJECT_DIR}",
    env=dbt_env,
    dag=dag,
)

dbt_run_staging = PythonOperator(
    task_id='dbt_run_staging',
    python_callable=run_dbt_with_performance,
    op_kwargs={'models_selector': 'Staging.*'},
    dag=dag,
)

dbt_run_dimensions = PythonOperator(
    task_id='dbt_run_dimensions',
    python_callable=run_dbt_with_performance,
    op_kwargs={'models_selector': 'Dimensions.*'},
    dag=dag,
)

# Parallel fact table execution with dedicated Python operators
dbt_run_balancesheet = PythonOperator(
    task_id='dbt_run_balancesheet',
    python_callable=run_dbt_with_performance,
    op_kwargs={'models_selector': 'Facts.fct_balanceSheet'},
    dag=dag,
    pool='high_cpu_tasks',  # If you define a resource pool in Airflow
    priority_weight=10  # Higher priority
)

dbt_run_incomestatement = PythonOperator(
    task_id='dbt_run_incomestatement',
    python_callable=run_dbt_with_performance,
    op_kwargs={'models_selector': 'Facts.fct_IncomeStatement'},
    dag=dag,
    pool='high_cpu_tasks',
    priority_weight=10
)

dbt_run_cashflows = PythonOperator(
    task_id='dbt_run_cashflows',
    python_callable=run_dbt_with_performance,
    op_kwargs={'models_selector': 'Facts.fct_Cashflows'},
    dag=dag,
    pool='high_cpu_tasks',
    priority_weight=10
)

dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_EXECUTABLE} test",
    env=dbt_env,
    dag=dag,
)

# Sequential workflow for dependencies
dbt_debug >> dbt_deps >> dbt_run_staging >> dbt_run_dimensions

# Parallel execution of fact tables
dbt_run_dimensions >> [dbt_run_balancesheet, dbt_run_incomestatement, dbt_run_cashflows]

# Wait for all fact tables to complete before running tests
[dbt_run_balancesheet, dbt_run_incomestatement, dbt_run_cashflows] >> dbt_test
