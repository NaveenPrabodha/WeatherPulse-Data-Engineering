from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

# Function for PythonOperator
def print_hello():
    print("Hello from Airflow!")

# Define default_args dictionary
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='simple_hello_dag',
    default_args=default_args,
    description='A simple DAG with dummy and python tasks',
    schedule_interval='@daily',  # Run once a day
    catchup=False,  # Skip running for past dates
) as dag:

    

    hello_task = PythonOperator(
        task_id='say_hello',
        python_callable=print_hello
    )

    

    # Define task dependencies
    hello_task
