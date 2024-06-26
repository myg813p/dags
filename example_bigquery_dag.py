# BigQuery DAG 예시 파일 

from airflow import DAG
from datetime import datetime
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 18),
}

# DATASET = "vernal-dispatch-420407.kiyoung_test" 
DATASET = "kiyoung_test" 
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
        gcp_conn_id='google_cloud_default'
    )



    create_dataset >> create_table
