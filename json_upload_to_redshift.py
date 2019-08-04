import airflow
from airflow import DAG
from airflow.operators import BashOperator, PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator





# Following are defaults which can be overridden later on
default_args = {
    'owner': 'Yue',
    'depends_on_past': False,
    'start_date': datetime(2019, 7, 20),
    'email': ['yhu242@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def read_json_2013_fn():
  print("hello from task13 read data")
  data_2013=pd.read_json("https://data.colorado.gov/resource/tv8u-hswn.json")
  print('shape of the dataframe is: ', data_2013.shape)
  data_2013.to_csv('data_2013.csv')

def write_csv_gcs_fn():
  os.system("gsutil cp data_2013.csv gs://us-central1-airflows-0e76080f-bucket/data/")

dag = DAG('json_upload_to_redshift', default_args=default_args)

read_json_2013 = PythonOperator(
    task_id='read_json_2013',
    python_callable=read_json_2013_fn,
    provide_context=False,
    dag=dag)

write_csv_gcs = PythonOperator(
    task_id='write_csv_gcs',
    python_callable=write_csv_gcs_fn,
    provide_context=False,
    dag=dag)

schema = [{
    "type": "STRING",
    "name": "id",
    "mode": "NULLABLE",
    "description": "The row ID"
         },
          {
              "type": "STRING",
              "name": "age",
              "mode": "NULLABLE",

          },
          {
              "type": "STRING",
              "name": "similar",
              "mode": "NULLABLE",

          },
          {
              "type": "STRING",
              "name": "datatype",
              "mode": "NULLABLE",

          },
          {
              "type": "STRING",
              "name": "femalepopulation",
              "mode": "NULLABLE",

          },
          {
              "type": "STRING",
              "name": "Fipscode",
              "mode": "NULLABLE",

          },
          {
              "type": "STRING",
              "name": "malepopulation",
              "mode": "NULLABLE",

          },
          {
              "type": "STRING",
              "name": "totalpopulation",
              "mode": "NULLABLE",
          },
          {
              "type": "String",
              "name": "year",
              "mode": "NULLABLE",

          }]

from_gcs_to_bigQuery = GoogleCloudStorageToBigQueryOperator(
    task_id='import_data',
    bucket='us-central1-airflows-0e76080f-bucket/data',
    source_objects=['*.csv'],
    destination_project_dataset_table='yueairflow.Jason_Yue.population',
    source_format='CSV',
    field_delimiter=',',
    max_bad_records=10,
    skip_leading_rows=1,
    schema_fields=schema,
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
    bigquery_conn_id='google_cloud_default',
    trigger_rule='all_success')

read_json_2013 >> write_csv_gcs >> from_gcs_to_bigQuery
