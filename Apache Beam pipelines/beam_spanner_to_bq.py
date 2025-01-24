import os
import argparse
import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.spanner import ReadFromSpanner, TimestampBoundMode
from typing import NamedTuple

# Set environment variables
PROJECT_ID = os.environ.get("GCP_PROJECT")
ENV = PROJECT_ID.split('-')[2] if PROJECT_ID else "dev" 

os.environ["GCP_PROJECT"] = f"shan-practice-{ENV}"
PROJECT_ID = os.environ.get("GCP_PROJECT")

# Custom pipeline options
class SpannerToBQLoader(PipelineOptions):
    """Pipeline options for Spanner to BigQuery loader."""

    @classmethod
    def _add_argparse_args(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument('--project_id', required=True, help='Google Cloud project ID for BigQuery.')
        parser.add_argument('--bq_dataset_id', required=True, help='BigQuery dataset to load to.')
        parser.add_argument('--bq_table_id', required=True, help='BigQuery table name to load to.')
        parser.add_argument('--spanner_project_id', required=True, help='Spanner project ID.')
        parser.add_argument('--spanner_instance_id', required=True, help='Spanner instance ID.')
        parser.add_argument('--spanner_database_id', required=True, help='Spanner database ID.')
        parser.add_argument('--spanner_table_id', required=True, help='Spanner table ID.')
        parser.add_argument('--gcs_temp_location', required=True, help='GCS temp location.')

# Define the Spanner row schema as a NamedTuple
class OrderRow(NamedTuple):
    OrderCode: str
    Status: str
    Comments: str

# Main pipeline
def run(argv=None):
    pipeline_options = PipelineOptions(argv)
    options = pipeline_options.view_as(SpannerToBQLoader)

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'ReadFromSpanner' >> ReadFromSpanner(
                instance_id=options.spanner_instance_id,
                database_id=options.spanner_database_id,
                project_id=options.spanner_project_id,
                sql=f"SELECT OrderCode, Status, Comments FROM {options.spanner_table_id}",
                row_type=OrderRow,
                batching=False,
                timestamp_bound_mode=TimestampBoundMode.STRONG
            )
            | 'ConvertToDict' >> beam.Map(lambda row: {
                "OrderCode": row.OrderCode,
                "Status": row.Status,
                "Comments": row.Comments or "NULL"
            })
            | 'WriteToBigQuery' >> WriteToBigQuery(
                table=f"{options.project_id}:{options.bq_dataset_id}.{options.bq_table_id}",
                custom_gcs_temp_location=options.gcs_temp_location,
                schema={
                    "fields": [
                        {"name": "OrderCode", "type": "STRING"},
                        {"name": "Status", "type": "STRING"},
                        {"name": "Comments", "type": "STRING"}
                    ]
                },
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == "__main__":
    run()
