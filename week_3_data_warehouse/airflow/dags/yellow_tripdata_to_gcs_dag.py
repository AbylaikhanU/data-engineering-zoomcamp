import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

import pyarrow.csv as pv
import pyarrow.parquet as pq
import pyarrow as pa

from datetime import datetime

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

# dataset_file = "yellow_tripdata_2021-01.csv"
# dataset_url = f"https://s3.amazonaws.com/nyc-tlc/trip+data/{dataset_file}"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

url_prefix = 'https://s3.amazonaws.com/nyc-tlc/trip+data'
url_template = url_prefix + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
output_file_template = 'yellow_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'

parquet_file = output_file_template.replace('.csv', '.parquet')

# def format_to_parquet(src_file):
#     if not src_file.endswith('.csv'):
#         logging.error("Can only accept source files in CSV format, for the moment")
#         return
#     table = pv.read_csv(src_file)
#     pq.write_table(table, src_file.replace('.csv', '.parquet'))


def change_dtype(local_file):
    raw_table = pq.read_table(local_file)
    df_raw = raw_table.to_pandas()
    schema = {
        'VendorID': pa.int64(),
        'tpep_pickup_datetime': pa.timestamp('ns'),
        'tpep_dropoff_datetime': pa.timestamp('ns'),
        'passenger_count': pa.float64(),
        'trip_distance': pa.float64(),
        'RatecodeID': pa.float64(),
        'store_and_fwd_flag': pa.string(),
        'PULocationID': pa.int64(),
        'DOLocationID': pa.int64(),
        'payment_type': pa.int64(),
        'fare_amount': pa.float64(),
        'extra': pa.float64(),
        'mta_tax': pa.float64(),
        'tip_amount': pa.float64(),
        'tolls_amount': pa.float64(),
        'improvement_surcharge': pa.float64(),
        'total_amount': pa.float64(),
        'congestion_surcharge': pa.float64(),
        'airport_fee': pa.float64()
    }

    fields = [pa.field(x, y) for x, y in schema.items()]
    new_schema = pa.schema(fields)

    table = pa.Table.from_pandas(
        df_raw,
        schema=new_schema,
        preserve_index=False
    )
    pq.write_table(table, local_file)


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
    "start_date": datetime(2019,1,1),
    "end_date": datetime(2020,12,31),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="yellow_tripdata_to_gcs_dag",
    schedule_interval="@monthly",
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['YellowTripdata'],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSLf {url_template} > {path_to_local_home}/{output_file_template}"
    )

    # format_to_parquet_task = PythonOperator(
    #     task_id="format_to_parquet_task",
    #     python_callable=format_to_parquet,
    #     op_kwargs={
    #         "src_file": f"{path_to_local_home}/{dataset_file}",
    #     },
    # )


    change_dtype_task=PythonOperator(
        task_id='change_dtype',
        python_callable=change_dtype,
        op_kwargs={
            'local_file':f"{path_to_local_home}/{parquet_file}"
        }
    )

    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{parquet_file}",
            "local_file": f"{path_to_local_home}/{parquet_file}",
        },
    )



    remove_temp_files_task = BashOperator(
        task_id='remove_temp_files',
        bash_command=f'rm {path_to_local_home}/{parquet_file}'
    )

    # bigquery_external_table_task = BigQueryCreateExternalTableOperator(
    #     task_id="bigquery_external_table_task",
    #     table_resource={
    #         "tableReference": {
    #             "projectId": PROJECT_ID,
    #             "datasetId": BIGQUERY_DATASET,
    #             "tableId": "external_table",
    #         },
    #         "externalDataConfiguration": {
    #             "sourceFormat": "PARQUET",
    #             "sourceUris": [f"gs://{BUCKET}/raw/{parquet_file}"],
    #         },
    #     },
    # )

    download_dataset_task >>  change_dtype_task >> local_to_gcs_task >> remove_temp_files_task
