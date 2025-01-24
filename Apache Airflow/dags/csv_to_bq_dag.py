from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import bigquery
from google.auth import impersonated_credentials, default
import pandas as pd
import numpy as np
import math

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

# Initialize the DAG
dag = DAG(
    dag_id='csv_to_bq',
    default_args=default_args,
    description='A DAG to preprocess CSV data and upload it to BigQuery',
    schedule_interval=None,  # Adjust your schedule as needed
)

# Function to get impersonated credentials
def get_impersonated_credentials():
    target_service_account = 'shan-practice-compute@shan-practice-dev.iam.gserviceaccount.com'
    source_credentials, project_id = default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
    target_credentials = impersonated_credentials.Credentials(
        source_credentials=source_credentials,
        target_principal=target_service_account,
        target_scopes=['https://www.googleapis.com/auth/cloud-platform']
    )
    return target_credentials, project_id

# Function to preprocess the CSV file
def preprocess_csv(**kwargs):
    file_path = kwargs['file_path']
    df = pd.read_csv(file_path, sep=',', low_memory=False, skiprows=0)

    df = df.fillna('Null')
    df.drop_duplicates(keep='first', inplace=True)

    if 'index' in df.columns:
        df.drop(['index'], axis=1, inplace=True)

    if 'Unnamed: 0' in df.columns:
        df.drop(['Unnamed: 0'], axis=1, inplace=True)

    df = df.applymap(lambda x: None if str(x).strip() == '' else x)
    df.columns = df.columns.str.replace(' ', '_')

    categorical_cols = df.select_dtypes(include=["object"]).columns.tolist()
    for col in categorical_cols:
        df[col] = df[col].fillna('None').astype(str).str.strip().replace({'None': None})

    numerical_cols = df.select_dtypes(include=["number"]).columns.tolist()
    for col in numerical_cols:
        df[col] = df[col].replace({np.nan: None})

    datetime_cols = df.select_dtypes(include=["datetime"]).columns.tolist()
    for col in datetime_cols:
        df[col] = df[col].replace({pd.NaT: None})

    kwargs['ti'].xcom_push(key='preprocessed_df', value=df.to_dict())  # Push data to XCom

# Function to create the BigQuery schema
def create_schema(**kwargs):
    df_dict = kwargs['ti'].xcom_pull(task_ids='preprocess_csv', key='preprocessed_df')
    df = pd.DataFrame.from_dict(df_dict)
    schema = []

    for col in df.select_dtypes(include=["datetime"]).columns.tolist():
        schema.append({'name': col, 'type': 'TIMESTAMP', 'mode': 'NULLABLE'})

    for col in df.select_dtypes(include=["int"]).columns.tolist():
        schema.append({'name': col, 'type': 'INTEGER', 'mode': 'NULLABLE'})

    for col in df.select_dtypes(include=["float"]).columns.tolist():
        schema.append({'name': col, 'type': 'FLOAT', 'mode': 'NULLABLE'})

    for col in df.select_dtypes(include=["object"]).columns.tolist():
        schema.append({'name': col, 'type': 'STRING', 'mode': 'NULLABLE'})

    kwargs['ti'].xcom_push(key='schema', value=schema)

# Function to upload data to BigQuery
def upload_to_bigquery(**kwargs):
    df_dict = kwargs['ti'].xcom_pull(task_ids='preprocess_csv', key='preprocessed_df')
    schema_dict = kwargs['ti'].xcom_pull(task_ids='create_schema', key='schema')

    df = pd.DataFrame.from_dict(df_dict)
    schema = [
        bigquery.SchemaField(field['name'], field['type'], mode=field['mode'])
        for field in schema_dict
    ]

    table_name = 'shan-practice-dev.my_dataset_1.my_table_1'  # Update table name
    chunk_size = 100000

    target_credentials, _ = get_impersonated_credentials()
    client = bigquery.Client(credentials=target_credentials)

    for i in range(0, df.shape[0], chunk_size):
        chunk = df.iloc[i:i + chunk_size]
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            autodetect=False,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )

        try:
            job = client.load_table_from_dataframe(chunk, table_name, job_config=job_config)
            job.result()
            print(f"Batch {i // chunk_size + 1} inserted successfully to {table_name}.")
        except Exception as e:
            print(f"Error: {e}")

# Define tasks
preprocess_task = PythonOperator(
    task_id='preprocess_csv',
    python_callable=preprocess_csv,
    op_kwargs={'file_path': 'gs://us-central1-my-composer-1-5a3d8835-bucket/dags/TO_UPLOAD/bq_data_for_table.csv'},
    dag=dag,  
)

create_schema_task = PythonOperator(
    task_id='create_schema',
    python_callable=create_schema,
    dag=dag,
)

upload_task = PythonOperator(
    task_id='upload_to_bigquery',
    python_callable=upload_to_bigquery,
    dag=dag,
)

# Set task dependencies
preprocess_task >> create_schema_task >> upload_task
