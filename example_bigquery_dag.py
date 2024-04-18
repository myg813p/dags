# BigQuery DAG 예시 파일 

from airflow import DAG
from datetime import datetime
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 4, 18),
}

DATASET = "simple_bigquery_example_dag" 
TABLE = "forestfires"

with DAG('example_bigquery_dag',
         default_args=default_args,
         schedule_interval=None) as dag:

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset", dataset_id=DATASET,
        gcp_conn_id='google_cloud_default'
    )

    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        dataset_id=DATASET,
        table_id=TABLE,
        schema_fields=[
            {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "y", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "month", "type": "STRING", "mode": "NULLABLE"},
            {"name": "day", "type": "STRING", "mode": "NULLABLE"},
        ],
        gcp_conn_id='rc_gcp_bq_conn'
    )



    create_dataset >> create_table
