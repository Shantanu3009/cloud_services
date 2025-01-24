import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
import logging
import re

class ComputeWordLength(beam.DoFn):
    def process(self, element):
        print(element)
        #input()
        return [len(element)]

class WordExreactDoFunc(beam.DoFn):
    def process(self, element):
        print(element)
        words = re.findall(r'[\w\']+',element, re.UNICODE)
        print(words)
        return words


def run(argv = None):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--input',
        dest = 'INPUT',
        default = 'gs://shantanu_practice_bucket/kinglear.txt'
    )
    parser.add_argument(
        '--output',
        dest = 'OUTPUT',
        required = True
    )
    print('Known and Pipeline arguments are:')
    print(parser.parse_known_args(argv))
   

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    print(f"I/P : {known_args.INPUT}")
    print(f"O/P : {known_args.OUTPUT}")
    
    with beam.Pipeline(options = pipeline_options) as p:
        output_pc = p | 'reading' >> ReadFromText(known_args.INPUT)
        output_pc = output_pc | "Extract" >> beam.ParDo(WordExreactDoFunc().with_output_types(str))
        output_pc = output_pc | "Pairwithone" >> beam.Map(lambda x : (x, 1))
        output_pc = output_pc | "GroupandSum" >> beam.CombinePerKey(sum)

        def format_result(word, count):
            return '%s: %d' % (word, count)

        out = output_pc | "finally" >> beam.MapTuple(format_result)
        out | 'write' >> WriteToText(known_args.OUTPUT)




if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    run()