import argparse
import json
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

# Set up logging
logging.basicConfig(level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)

# Define a custom DoFn for parsing PubSub messages
class CustomParsing(beam.DoFn):
    def process(self, element: bytes, timestamp=beam.DoFn.TimestampParam, window=beam.DoFn.WindowParam):
        message = json.loads(element.decode("utf-8"))
        result = {}
        try:
            # Extract necessary fields from the message
            result['Name'] = message['protoPayload']['name']
            result['Email'] = message['protoPayload']['authenticationInfo']['email']
            result['serviceName'] = message['protoPayload']['serviceName']
            result['Type'] = message['protoPayload']['type']
            result['timestamp'] = message['timestamp']
            yield result
        except Exception as e:
            # Log errors for debugging
            logging.error(f"Failed to process message: {message}. Error: {str(e)}")
            return

# Define custom pipeline options for PubSub to BigQuery
class PubSubBigQueryOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--input_topic", required=True, help="PubSub topic to read messages from")
        parser.add_argument("--output_table", required=True, help="BigQuery table to write data to")
        parser.add_argument("--table_schema", required=True, help="Schema for the BigQuery table")

# Main function to define the pipeline
def run():
    # Get pipeline options
    pipeline_options = PipelineOptions()
    pubsub_bigquery_options = pipeline_options.view_as(PubSubBigQueryOptions)
    pipeline_options.view_as(StandardOptions).streaming = True

    # Build the pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "ReadFromPubSub" >> beam.io.gcp.pubsub.ReadFromPubSub(
                topic=pubsub_bigquery_options.input_topic,
                timestamp_attribute=None
            )
            | "CustomParse" >> beam.ParDo(CustomParsing())
            | "WriteToBigQuery" >> beam.io.gcp.bigquery.WriteToBigQuery(
                table=pubsub_bigquery_options.output_table,
                schema=pubsub_bigquery_options.table_schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
        )

# Entry point
if __name__ == "__main__":
    run()
