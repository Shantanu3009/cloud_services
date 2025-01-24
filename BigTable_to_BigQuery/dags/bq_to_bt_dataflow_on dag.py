import os
import sys
import json
# Add current directory to sys.path for module import
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# Importing required modules
from airflow import models
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
import logging

"""
os.system('pip3 install pendulum')
import pendulum
"""


# Import configuration and email notification context
#import email_context as ec

#folder paths
DAG_PATH = os.environ.get('DAGS_FOLDER', '/home/airflow/gcs/dags')
if not os.path.exists(DAG_PATH):
    raise ValueError(f"DAG_PATH does not exist: {DAG_PATH}")

PROJECT_ID = os.environ.get("GCP_PROJECT")  # Set GCP Project ID dynamically
REPO_NAME = "BigTable_to_BigQuery_DAG"
py_folder_path = os.path.join(DAG_PATH, f"{REPO_NAME}/dags/src/py")



PROJECT_PARAMS_PATH = os.path.join(DAG_PATH, f"{REPO_NAME}/dags/src/config/config.json")
with open(PROJECT_PARAMS_PATH, "r") as file:
    ct = json.load(file)

# Constants and Configuration Variables
REGION = ct['config']['REGION']  # "us-central1"
USER = ct['config']['USER'] # "shantanu"



# Bigtable Configuration
bt_instance_id = ct['config']['bt_instance_id']['bt_db_instance_id']
bt_store = ct['config']['bt_store']['bt_procedure_store']

# Buckets
code_bucket = ct['config']['BUCKETS']['CODE_BUCKET']
data_bucket = ct['config']['BUCKETS']['DATA_BUCKET']

# Default arguments for DAG
default_dag_args = {
    "start_date": days_ago(1),
    "project_id": PROJECT_ID,
    "retries": 0,
    "description": "A DAG to ingest Data from BigTable to Bigquery using DataFlow and Apache Beam",
    "email_on_failure": False,
    "depends_on_past": False,
    "email_on_retry": False,
    #"dataflow_kms_key": f"projects/{PROJECT_ID}/locations/{REGION}/keyRings/{KMS_KEYRING}/cryptoKeys/{PROJECT_ID}"
}

# Define the DAG
with models.DAG(
    dag_id=f"{REPO_NAME}-dag-dataflow",
    default_args=default_dag_args,
    start_date=days_ago(1),
    schedule_interval=None,  # Cron expression for scheduling
    catchup=False,
    template_searchpath= py_folder_path,  # this ensures that airflow checks the composer DAG bucket for python files
    tags=[f"owner:{USER}"]
) as dag:

    # Task: BQ to Bigtable
    bq_bt_convert_procedure= BeamRunPythonPipelineOperator(
        task_id="bq_bt_convert_procedure",
        runner="DataflowRunner",
        py_file=f"{py_folder_path}/bq_to_bt.py",
        pipeline_options={
            "tempLocation": f"gs://{code_bucket}/",
            "maxNumWorkers": "2",
            "subnetwork": f"regions/{REGION}/subnetworks/{ct['config']['subnetwork_uri']}",
            "no_use_public_ips": None,
            "worker_zone": f"{REGION}-a",
            "impersonate_service_account": ct['config']['connect_sa'],
            "service_account_email": ct['config']['resource_sa'],
            "stagingLocation": f"gs://{data_bucket}/stg-temp-dataflow",
            "input_query": f"SELECT time as key, player_name, location, count, games FROM {PROJECT_ID}.{ct['config']['bq_dataset']['bq_dataset_id']}.{ct['config']['bq_table_names']['bq_table']}",
            "bq_dataset_id": ct['config']['bq_dataset']['bq_dataset_id'],
            "bt_project_id": PROJECT_ID,
            "bt_instance_id": bt_instance_id,
            "bt_table_id": bt_store,
            #"bt_column_name": "name",
            #"bt_column_family": "cf1",
        },
        py_options=[],
        py_requirements=['apache-beam[gcp]==2.61.0', 'pendulum==3.0.0', 'google-cloud-bigtable==2.27.0', 'google-cloud-storage==2.19.0', 'google-cloud-bigquery==3.27.0'],  #==2.61.0
        py_interpreter="python3.11.8",
        py_system_site_packages=False,
        dataflow_config=DataflowConfiguration(
            job_name="bq_bt_convert_procedure_1",
            location=REGION,
            project_id=PROJECT_ID,
            impersonation_chain=ct['config']['connect_sa'],
            drain_pipeline=False,
            cancel_timeout=600,
            wait_until_finished=True,
        ),
    )
    
    bq_bt_convert_procedure
    
