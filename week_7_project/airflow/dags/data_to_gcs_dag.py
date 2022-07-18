import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator 

import pyarrow.csv as pv
import pyarrow.parquet as pq
import pyarrow as pa

from datetime import datetime

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
# BUCKET = os.environ.get("GCP_GCS_BUCKET")
BUCKET = "dtc_personal_project_de-zcamp-1234"

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "bq_personal_project")

src_file_csv='new_york_times_data.csv'
src_file_pq='new_york_times_data.parquet'

dataset_url = "https://www.dropbox.com/s/hfee1gzk5d51vh4/nytimes%20front%20page.csv?dl=0"

def process_to_parquet(local_file):
    table = pv.read_csv(local_file)
    table = table.drop([''])
    pq.write_table(table, src_file_pq)

# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2022,7,4),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="personal_project_dag",
    schedule_interval="@once",
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['PersonalProject']
) as dag:

    download_dataset_task = BashOperator(
        task_id='download_dataset_task',
        bash_command=f"curl -sSLf {dataset_url} > {path_to_local_home}/{src_file_csv}"
    )

    process_to_parquet_task = PythonOperator(
        task_id='process_to_parquet_task',
        python_callable=process_to_parquet,
        op_kwargs={
            "local_file":f"{src_file_csv}"
        }
    )

    upload_to_gcs_task = PythonOperator(
        task_id="upload_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{src_file_pq}",
            "local_file": f"{path_to_local_home}/{src_file_pq}",
        }
    )

    data_to_external_bq_task = BigQueryCreateExternalTableOperator(
        task_id="data_to_external_bq_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "NY_times_data",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{src_file_pq}"],
            },
        },
    )

    CREATE_TBL_BQ_QUERY = (
        f'''CREATE OR REPLACE TABLE {PROJECT_ID}.{BIGQUERY_DATASET}.NY_times_data_partitioned 
          PARTITION BY 
            DATE_TRUNC(date, MONTH)   
                AS 
                SELECT * FROM {PROJECT_ID}.{BIGQUERY_DATASET}.NY_times_data;'''
    )

    create_partitioned_table_task = BigQueryInsertJobOperator(
        task_id='create_partitioned_table_task',
        configuration={
            'query':{
                'query':CREATE_TBL_BQ_QUERY,
                'useLegacySql':False
            }
        }
    )

    download_dataset_task >> process_to_parquet_task >> upload_to_gcs_task >> \
        data_to_external_bq_task >> create_partitioned_table_task