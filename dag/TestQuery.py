

from datetime import timedelta, datetime


from airflow import DAG 
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator


GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID="dilipgcp"
GS_PATH = "gcpairflow/"
BUCKET_NAME = 'b_gcp_airflow'
STAGING_DATASET = "test_staging_dataset"
DATASET = "test_dataset"
LOCATION = "us-central1"

default_args = {
    'owner': 'Dilip Amarasekera',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date':  days_ago(1),
    'retry_delay': timedelta(minutes=5),
}

with DAG('TestQuery', schedule_interval=timedelta(days=1), default_args=default_args) as dag:
    start_pipeline = DummyOperator(
        task_id = 'start_pipeline',
        dag = dag
        )


    start_staging_data = DummyOperator(
        task_id = 'start_staging_data',
        dag = dag
        )    
    
    check_staged_dataset = BigQueryCheckOperator(
        task_id = 'check_staged_dataset',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `dilipgcp.test_staging_dataset.STG_D_10000`' 
        ) 

    
    
start_pipeline >> start_staging_data >> check_staged_dataset
