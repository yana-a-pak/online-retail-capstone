from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils import timezone
from datetime import datetime
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
#from airflow.providers.postgres.hooks.postgres import PostgresHook

import os
import glob


with DAG(
    "etl",
    start_date=timezone.datetime(2024,5,8),
    schedule_interval="@daily",
    tags=["retaila"],
) as dag:

# Dummy start task   
    start = DummyOperator(
        task_id='start',
        dag=dag,
    )


# Task to load data into Google Cloud Storage
    T1 = upload_csv_to_gcs = GCSToGCSOperator(
        task_id="upload_csv_to_gcs",
        source_bucket="raw-data-online-retaila",
        source_objects=["Online_Retail.csv"],
        destination_bucket="ds525-dw-capstone-11042024a",
        destination_object="Online_Retail.csv",
        gcp_conn_id='my_gcp_conn'
        
    )

#Create an empty Dataset 
    T2 = create_online_retail_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id ='create_online_retail_dataset',
        dataset_id ='raw_invoicesa',
        gcp_conn_id ='my_gcp_conn',
    )

#Create the task to load the file into a BigQuery "raw_invoices" table
    T3 = gcs_to_bq_online_retail = GCSToBigQueryOperator(
        task_id = "gcs_to_bq_online_retail",
        bucket = 'ds525-dw-capstone-11042024a',
        source_objects = ['Online_Retail.csv'],
        destination_project_dataset_table ='raw_invoicesa.retail',
        write_disposition ='WRITE_TRUNCATE',
        create_disposition = 'CREATE_IF_NEEDED',
        gcp_conn_id ='my_gcp_conn'
    )

# Dummy end task
    end = DummyOperator(
        task_id='end',
        dag=dag,
    )
 
    start >> T1 >> T2 >> T3 >> end