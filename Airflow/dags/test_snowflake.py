from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Create the DAG
with DAG(
    dag_id='verify_snowflake_connection',
    default_args=default_args,
    description='A simple DAG to verify Snowflake connection',
    schedule_interval=None,  # Set to None to run manually
    catchup=False,
) as dag:

    # Task to test the Snowflake connection
    test_snowflake_connection = SnowflakeOperator(
        task_id='test_snowflake_connection',
        snowflake_conn_id='snowflake_default',  # Use your Snowflake connection ID
        sql="SELECT CURRENT_TIMESTAMP;",  # Basic SQL query to test the connection
    )

    test_snowflake_connection
