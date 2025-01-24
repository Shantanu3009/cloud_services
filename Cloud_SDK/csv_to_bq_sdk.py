from google.cloud import bigquery
from google.auth import impersonated_credentials, default
import pandas as pd
import numpy as np
import math


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
    file_path = 'orders.csv'
    df = pd.read_csv(file_path, sep=',', low_memory=False, skiprows=0)
    print(f"Reading from filepath: {file_path} and dataframe has shape {df.shape}")
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

    schema = []

    for col in df.select_dtypes(include=["datetime"]).columns.tolist():
        schema.append({'name': col, 'type': 'TIMESTAMP', 'mode': 'NULLABLE'})

    for col in df.select_dtypes(include=["int"]).columns.tolist():
        schema.append({'name': col, 'type': 'INTEGER', 'mode': 'NULLABLE'})

    for col in df.select_dtypes(include=["float"]).columns.tolist():
        schema.append({'name': col, 'type': 'FLOAT', 'mode': 'NULLABLE'})

    for col in df.select_dtypes(include=["object"]).columns.tolist():
        schema.append({'name': col, 'type': 'STRING', 'mode': 'NULLABLE'})

    schema_dict = [
        bigquery.SchemaField(field['name'], field['type'], mode=field['mode'])
        for field in schema
    ]

    table_name = 'shan-practice-dev.my_dataset_bq_1.my_table_orders'  # Update table name
    chunk_size = 100000

    client = bigquery.Client(project="shan-practice-dev")

    for i in range(0, df.shape[0], chunk_size):
        chunk = df.iloc[i:i + chunk_size]
        job_config = bigquery.LoadJobConfig(
            schema=schema_dict,
            autodetect=False,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )

        try:
            job = client.load_table_from_dataframe(chunk, table_name, job_config=job_config)
            job.result()
            print(f"Batch {i // chunk_size + 1} inserted successfully to {table_name}.")
        except Exception as e:
            print(f"Error: {e}")

if __name__ == '__main__':
    preprocess_csv()
