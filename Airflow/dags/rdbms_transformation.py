from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

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

# Run `dbt debug` to test the connection
dbt_debug = BashOperator(
    task_id='dbt_debug',
    bash_command=f"cd {DBT_PROJECT_DIR} && dbt debug",
    dag=dag,
)

# Run `dbt run` to execute models
dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command=f"cd {DBT_PROJECT_DIR} && dbt run",
    dag=dag,
)

# Run `dbt test` to validate transformations
dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command=f"cd {DBT_PROJECT_DIR} && dbt test",
    dag=dag,
)

# DAG Execution Order
dbt_debug >> dbt_run >> dbt_test
