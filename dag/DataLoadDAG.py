

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

with DAG('TestETL', schedule_interval=timedelta(days=1), default_args=default_args) as dag:
    start_pipeline = DummyOperator(
        task_id = 'start_pipeline',
        dag = dag
        )


    start_staging_data = DummyOperator(
        task_id = 'start_staging_data',
        dag = dag
        )    
    
    load_stage_dataset = GCSToBigQueryOperator(
        task_id = 'load_stage_dataset',
        bucket = BUCKET_NAME,
        source_objects = ['gcpairflow/10000-Records.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.STG_D_10000',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1
        # ,
        # schema_fields=[
        # {'name': 'Title', 'type': 'STRING', 'mode': 'REQUIRED'},
        # {'name': 'Ingredients', 'type': 'STRING', 'mode': 'NULLABLE'},
        # {'name': 'Steps', 'type': 'STRING', 'mode': 'NULLABLE'},
        # {'name': 'Loves', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        # {'name': 'URL', 'type': 'STRING', 'mode': 'NULLABLE'},
        #     ]
        )
    
    check_staged_dataset = BigQueryCheckOperator(
        task_id = 'check_staged_dataset',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.STG_D_10000`' 
        ) 

    start_loading_data = DummyOperator(
        task_id = 'start_loading_data',
        dag = dag
        )

    create_data_table = BigQueryOperator(
        task_id = 'create_data_table',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/test_data/D_10000.sql'
        )

    check_loaded_data = BigQueryCheckOperator(
        task_id = 'check_loaded_data',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{DATASET}.D_10000`'
        ) 

    finish_pipeline = DummyOperator(
        task_id = 'finish_pipeline',
        dag = dag
        ) 
    
start_pipeline >> start_staging_data >> load_stage_dataset 

load_stage_dataset >> check_staged_dataset

check_staged_dataset >> start_loading_data

start_loading_data >> create_data_table 

create_data_table >> check_loaded_data

check_loaded_data >> finish_pipeline
