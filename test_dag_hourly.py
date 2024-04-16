from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def print_message():
    print("Hello, this is a test message from Airflow!")

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 16),
    'retries': 1,
}

# Define the DAG with an hourly schedule interval
with DAG('test_dag_hourly', 
         default_args=default_args, 
         schedule_interval='@hourly', 
         catchup=False) as dag:

    # Task to print a message
    print_message_task = PythonOperator(
        task_id='print_message_task',
        python_callable=print_message
    )

    # Define the task dependencies
    print_message_task

# You can optionally define additional tasks and dependencies here
