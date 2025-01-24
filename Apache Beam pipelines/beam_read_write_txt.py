from __future__ import absolute_import
import argparse
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def parse_json(line):
    """Parses a line of text as JSON."""
    return json.loads(line)


def compute(record):
    """Processes each record to compute the desired output."""
    yield '{} {} {} {} {}'.format(
        record['order']['code'],
        len(record['order']['myRelatedOrders']),
        len(record['order']['statuses']),
        len(record['order']['partners']),
        len(record['order']['comments'])
    )


def run(argv=None):
    """Main entry point to run the pipeline."""
    parser = argparse.ArgumentParser()

    parser.add_argument('--input',
                        dest='input',
                        required=True,
                        help='Input file to process.')
    parser.add_argument('--output',
                        dest='output',
                        required=True,
                        help='Output file to write results to.')

    known_args, pipeline_args = parser.parse_known_args(argv)

    # Set up pipeline options
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'ReadInput' >> beam.io.ReadFromText(known_args.input)
            | 'ParseJson' >> beam.Map(parse_json)
            | 'ComputeMetrics' >> beam.FlatMap(compute)
            | 'WriteOutput' >> beam.io.WriteToText(known_args.output, shard_name_template='')
        )


if __name__ == '__main__':
    run()
