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
    'json_dbt_transformation',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

# Define the path to your DBT project and executable
DBT_PROJECT_DIR = "/opt/airflow/json_transform"
DBT_EXECUTABLE = "/home/airflow/.local/bin/dbt"

# Hard-code DBT_PROFILES_DIR in the command to ensure the correct profiles.yml is used
dbt_debug = BashOperator(
    task_id='dbt_debug',
    bash_command=(
        f"cd {DBT_PROJECT_DIR} && "
        f"export DBT_PROFILES_DIR={DBT_PROJECT_DIR} && "
        f"echo 'DBT_PROFILES_DIR is set to:' $DBT_PROFILES_DIR && "
        f"{DBT_EXECUTABLE} debug"
    ),
    dag=dag,
)

dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command=(
        f"cd {DBT_PROJECT_DIR} && "
        f"export DBT_PROFILES_DIR={DBT_PROJECT_DIR} && "
        f"{DBT_EXECUTABLE} run"
    ),
    dag=dag,
)

dbt_debug >> dbt_run
