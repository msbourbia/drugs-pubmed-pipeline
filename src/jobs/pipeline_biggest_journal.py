import sys
import argparse
import logging

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from src.transforms.biggest_journal import *

class PipelineJob:
    def run(self, argv=None, save_main_session=True):
        """Main entry point; defines and runs the pipeline."""
        parser = argparse.ArgumentParser()
        parser.add_argument(
            '--json_file_path',
            dest='json_file_path',
            default='json_file_path',
            help='jobs json input file path.')

        parser.add_argument(
            '--result_file_path',
            dest='result_file_path',
            default='result_file_path',
            help='job output file path.')

        args, pipeline_args = parser.parse_known_args(argv)
        options = PipelineOptions(pipeline_args)

        # We use the save_main_session option because one or more DoFn's in this
        # workflow rely on global context (e.g., a module imported at module level).
        options.view_as(SetupOptions).save_main_session = save_main_session

        with beam.Pipeline(options=options) as p:
            biggest_jouranl = p | 'read_data' >> beam.io.ReadFromText(args.json_file_path, coder=JsonCoder()) \
                                | 'FoundBiggestJournal transform' >> FoundBiggestJournal()

            biggest_jouranl | "print" >> beam.Map(print)
            biggest_jouranl | 'Write Output' >> beam.io.WriteToText(args.result_file_path, file_name_suffix=".txt",
                                                                    shard_name_template='')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    PipelineJob().run(argv=sys.argv)
