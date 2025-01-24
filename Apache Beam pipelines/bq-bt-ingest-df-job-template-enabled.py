import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigtableio import WriteToBigTable
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery
import argparse
import logging

logging.basicConfig(level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)


class BQToBTLoader(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            '--input_query',
            help='Query to read data from BigQuery'
        )
        parser.add_argument(
            '--bt_project_id',
            default='',
            help='GCP project ID for Bigtable'
        )
        parser.add_argument(
            '--bt_instance_id',
            default='',
            help='Bigtable instance ID'
        )
        parser.add_argument(
            '--bt_table_id',
            default='',
            help='Bigtable table ID'
        )


class CreateRowFn(beam.DoFn):
    def __init__(self, column_mappings):
        """
        Initialize with column mappings that specify which columns go to which families and columns.
        Args:
            column_mappings (dict): A dictionary mapping column families to their respective columns.
        """
        self.column_mappings = column_mappings

    def process(self, record):
        """
        Transform a BigQuery row into a Bigtable DirectRow.
        Args:
            record (dict): A dictionary representing a row fetched from BigQuery.
        Yields:
            DirectRow: A Bigtable row ready to be written.
        """
        from google.cloud.bigtable import row

        # Initialize the Bigtable row with the key
        row_key = str(record.get('key', 'default_key')).encode('utf-8')
        direct_row = row.DirectRow(row_key=row_key)

        # Populate the Bigtable row based on column mappings
        for family, columns in self.column_mappings.items():
            for column in columns:
                if column in record:
                    value = str(record[column]).encode('utf-8')
                    direct_row.set_cell(family, column, value)

        yield direct_row


def run():
    # Parse pipeline options
    pipeline_options = PipelineOptions()
    bq_bt_options = pipeline_options.view_as(BQToBTLoader)

    # Define column family and column mappings
    column_mappings = {
        "data": ["player_name", "location"],  # Columns for the "data" family
        "stats": ["time", "count"],   # Columns for the "stats" family
    }

    # Initialize the pipeline
    p = beam.Pipeline(options=pipeline_options)

    # Build the pipeline
    (
        p
        | 'Read From BQ' >> beam.io.ReadFromBigQuery(
            query=bq_bt_options.input_query,
            project = bq_bt_options.bt_project_id,
            use_standard_sql=True
        )
        | 'Transform to Bigtable Rows' >> beam.ParDo(CreateRowFn(column_mappings))
        | 'Write to Bigtable' >> WriteToBigTable(
            project_id=bq_bt_options.bt_project_id,
            instance_id=bq_bt_options.bt_instance_id,
            table_id=bq_bt_options.bt_table_id
        )
    )

    # Run the pipeline
    p.run().wait_until_finish()


if __name__ == '__main__':
    run()



