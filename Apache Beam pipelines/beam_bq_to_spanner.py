import os
import argparse
import google.auth
from google.cloud import bigquery, spanner
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.experimental.spannerio import WriteToSpanner, WriteMutation



class BQToSpannerLoader(PipelineOptions):
    """
    Custom pipeline options for configuring BigQuery-to-Spanner load.
    """
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--input_query',
            required=True,
            help='Query to fetch data from BigQuery.'
        )
        parser.add_argument(
            '--project_id',
            required=True,
            help='Google Cloud project ID for BigQuery.'
        )
        parser.add_argument(
            '--spanner_project_id',
            required=True,
            help='Google Cloud project ID for Spanner.'
        )
        parser.add_argument(
            '--spanner_instance_id',
            required=True,
            help='Spanner instance ID.'
        )
        parser.add_argument(
            '--spanner_database_id',
            required=True,
            help='Spanner database ID.'
        )
        parser.add_argument(
            '--spanner_table_id',
            required=True,
            help='Spanner table ID.'
        )
        parser.add_argument(
            '--spanner_table_pk',
            required=True,
            help='Spanner table primary key (comma-separated for composite keys).'
        )
        parser.add_argument(
            '--spanner_drop_indexes',
            action='append',
            required=False,
            default=[],
            help='List of Spanner indexes to drop before loading data.'
        )
        parser.add_argument(
            '--spanner_create_indexes',
            action='append',
            required=False,
            default=[],
            help='List of Spanner indexes to create after loading data.'
        )


class CreateRowFn(beam.DoFn):
    """
    A Beam DoFn for transforming BigQuery rows into Spanner mutations.
    """
    def __init__(self, spanner_table_id):
        self.spanner_table_id = spanner_table_id

    def process(self, d):
        # Convert all column values to strings to ensure compatibility
        d = {key: str(d[key]) for key in d.keys()}
        
        # Create a WriteMutation object for Spanner
        yield WriteMutation.insert_or_update(
            table=self.spanner_table_id,
            columns=list(d.keys()),    # Column names
            values=[list(d.values())] # Row values
        )


def run(argv=None):
    """
    Main entry point for the pipeline.
    """
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    bq_spanner_options = pipeline_options.view_as(BQToSpannerLoader)

    # Create the pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            # Read from BigQuery
            | 'ReadFromBigQuery' >> beam.io.ReadFromBigQuery(
                query=bq_spanner_options.input_query,
                use_standard_sql=True,
                project=bq_spanner_options.project_id
            )
            # Transform rows for Spanner
            | 'TransformRows' >> beam.ParDo(CreateRowFn(bq_spanner_options.spanner_table_id))
            # Write to Spanner
            | 'WriteToSpanner' >> WriteToSpanner(
                project_id=bq_spanner_options.spanner_project_id,
                instance_id=bq_spanner_options.spanner_instance_id,
                database_id=bq_spanner_options.spanner_database_id
            )
        )

if __name__ == '__main__':
    run()
