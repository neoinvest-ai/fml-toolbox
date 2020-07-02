import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from fml.data.parse import Tick


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        type=str,
        required=True,
        help='Path to data file(s) containing tick data')
    parser.add_argument(
        '--output',
        type=str,
        required=True,
        help='Path to the output file(s)')

    known_args, pipeline_args = parser.parse_known_args(argv)
    options = PipelineOptions(pipeline_args)
    # Used as one or more DoFn's rely on global context (e.g module imported
    # at module level)
    options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=options) as p:
        (
                p
                | 'ReadInputFile' >> beam.io.ReadFromText(known_args.input)
                | 'ParseTicks' >> Tick()
        )
