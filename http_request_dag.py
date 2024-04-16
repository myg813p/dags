from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests

# Define the function to make an HTTP request and save response to a file
def make_http_request_and_save_response():
    url = 'https://jsonplaceholder.typicode.com/posts/1'
    response = requests.get(url)
    if response.status_code == 200:
        print("HTTP request successful!")
        print("Response:")
        print(response.json())
        
        # Save response to a file
        # with open('/path/to/response.json', 'w') as f:
        #    json.dump(response.json(), f)
        #    print("Response saved to response.json")
    else:
        print(f"HTTP request failed with status code: {response.status_code}")

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date': datetime(2024, 4, 13, 0, 0),
    'timezone': 'Asia/Seoul',  # Set timezone to Asia/Seoul
}

# Instantiate the DAG with default arguments
dag = DAG(
    'http_request_dag',
    default_args=default_args,
    description='A DAG to make an HTTP request using requests library and save response to a file',
    schedule_interval='* 1 * * *',  # Run every 2 minutes
    catchup=False,
)

# Define the task to make the HTTP request and save response to a file
make_request_task = PythonOperator(
    task_id='make_http_request_and_save_response',
    python_callable=make_http_request_and_save_response,
    dag=dag,
)

# Set up task dependencies
make_request_task
