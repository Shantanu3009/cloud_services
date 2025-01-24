import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.bigtableio import ReadFromBigtable
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from google.cloud.bigtable import row_filters
from datetime import datetime
import os
import logging

# Set environment variables
os.environ["GCLOUD_PROJECT"] = "shan-practice-dev"
PROJECT_ID = os.environ.get("GCLOUD_PROJECT")
REGION = 'us-central1'

# Config variables
bq_secure_dataset = "my_dataset_bq_1"
bigquery_table = "my_table_claims_new"
bt_instance_id = "my_bq_instance"
table_id = "my_claims_bt_tbl"

# Example schema  #line_no:INTEGER,svc_from_dt:TIMESTAMP, load_date:DATE
bigquery_schema = """
    claim_date:DATE, claim_id:STRING, claim_segment_no:STRING ,claim_cd:STRING, unique_id:STRING, source:STRING 
    run_date:DATE, process_date:TIMESTAMP, file_cd:STRING ,load_year:STRING, load_month:STRING, load_date:DATE 
"""

# Logging setup
logging.basicConfig(level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)

# Timestamp conversion
dt_obj_start = datetime.strptime('11.01.2024 09:38:42,76', '%d.%m.%Y %H:%M:%S,%f')
start_timestamp_micros = int(dt_obj_start.timestamp() * 1000000)  # Convert to microseconds

dt_obj_end = datetime.strptime('17.01.2024 09:38:42,76', '%d.%m.%Y %H:%M:%S,%f')
end_timestamp_micros = int(dt_obj_end.timestamp() * 1000000)  # Convert to microseconds

# Transformation function
def process_data(row):
    key = row.row_key.decode('utf-8')
    claim_date = row.cell_value("cf1", b'CLAIM_DATE')
    claim_cd = row.cell_value("cf1", b'CLAIM_CD')
    claim_id = row.cell_value("cf1", b'CLAIM_ID')
    claim_segment_no = clmid[:2]  # First two characters
    year = row.cell_value("cf1", b'YEAR')
    month = row.cell_value("cf1", b'MONTH')
    uid = row.cell_value("cf1", b'UID')
    source = "CLAIM_SYSTEM"
    load_date = datetime.now().date()
    day_timestamp = datetime.now()

    return {
        'claim_date': claim_date,
        'claim_id': claim_id,
        'claim_segment_no': claim_segment_no,
        'claim_cd': claim_cd,
        'unique_id': uid,
        'source': source,
        'run_date': load_date,
        'process_date': day_timestamp,
        'file_cd': '',
        'load_year': year,
        'load_month': month,
        'load_date': load_date
    }

# Custom DoFn for filtering records
class FilterRecords(beam.DoFn):
    def process(self, row, start_timestamp, end_timestamp):
        for family, cells in row.cells.items():
            for qualifier, cell in cells.items():
                timestamp_micros = cell.timestamp_micros
                if start_timestamp <= timestamp_micros < end_timestamp:
                    yield row

# Main pipeline
def run_pipeline():
    options = PipelineOptions()
    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadFromBigTable" >> ReadFromBigtable(
                project_id=PROJECT_ID,
                instance_id=bt_instance_id,
                table_id=table_id
            )
            | "FilterRecords" >> beam.ParDo(FilterRecords(), start_timestamp=start_timestamp_micros, end_timestamp=end_timestamp_micros)
            | "ProcessData" >> beam.Map(process_data)
            | "WriteToBigQuery" >> WriteToBigQuery(
                table=f'{PROJECT_ID}:{bq_secure_dataset}.{bigquery_table}',
                schema=bigquery_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == "__main__":
    run_pipeline()
