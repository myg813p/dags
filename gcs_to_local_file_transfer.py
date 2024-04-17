from airflow import DAG
from airflow.operators import GoogleCloudStorageToLocalFilesystemOperator
from datetime import datetime
from airflow.utils.dates import cron
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 17),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'gcs_to_local_file_transfer',
    default_args=default_args,
    description='A DAG to transfer a file from Google Cloud Storage to the local filesystem',
    schedule_interval=cron('* * * * *'),  # Run the DAG every minute
)

# Define the content of your-file.txt
file_content = "This is the content of your-file.txt."

# Generate the filename based on the current date and time
current_datetime = datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
filename = f"/{current_datetime}.txt"

# Write the content to the dynamically generated filename
with open(filename, 'w') as f:
    f.write(file_content)

# Define the task to transfer the file from GCS to local filesystem
transfer_gcs_to_local_task = GoogleCloudStorageToLocalFilesystemOperator(
    task_id='transfer_gcs_to_local',
    bucket='kiyoung_storage',  # Replace with your GCS bucket name
    object=filename,           # Use the same filename for both object and local filename
    filename=filename,         # Use the same filename for both object and local filename
    dag=dag,
)

transfer_gcs_to_local_task
