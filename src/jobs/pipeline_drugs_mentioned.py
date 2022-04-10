import sys
import argparse

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from src.transforms.drugs_mentioned import *


class PipelineJob:
    def run(self, argv=None, save_main_session=True):
        """Main entry point; defines and runs the hourly_team_score pipeline."""
        parser = argparse.ArgumentParser()
        parser.add_argument(
            '--drugs_file_path',
            dest='drugs_file_path',
            default='drugs',
            help='drugs list input file path.')
        parser.add_argument(
            '--pubmed_file_path',
            dest='pubmed_file_path',
            default='pubmed',
            help='pubmed list input file path.')

        parser.add_argument(
            '--clinical_trials_file_path',
            dest='clinical_trials_file_path',
            default='clinical_trials',
            help='clinical_trials input file path.')

        parser.add_argument(
            '--result_file_path',
            dest='result_file_path',
            default='test',
            help='result json job output file path.')

        args, pipeline_args = parser.parse_known_args(argv)

        options = PipelineOptions(pipeline_args)

        # We use the save_main_session option because one or more DoFn's in this
        # workflow rely on global context (e.g., a module imported at module level).
        options.view_as(SetupOptions).save_main_session = save_main_session

        with beam.Pipeline(options=options) as p:
            pubmed = p | 'Read pubmed source file' >> beam.io.ReadFromText(args.pubmed_file_path, skip_header_lines=True) \
                     | 'get drug > pubmed, journal' >> PubmedGraph(args.drugs_file_path) \

            clinical_trials = p | 'Read clinical_trials source file' >> beam.io.ReadFromText(
                args.clinical_trials_file_path) \
                              | 'get drug > clinical_trials, journal' >> ClinicalTrialsGraph(args.drugs_file_path)

            journal_pubmed = pubmed | beam.Map(lambda elem: ([
                elem[0],
                {PUBMED_DATE: elem[1][PUBMED_DATE],
                 PUBMED_JOURNAL: elem[1][PUBMED_JOURNAL]
                 }]))
            journal_clinical_trials = clinical_trials | beam.Map(lambda elem: ([
                elem[0],
                {CLINICAL_TRIAL_DATE: elem[1][CLINICAL_TRIAL_DATE],
                 CLINICAL_TRIAL_JOURNAL: elem[1][CLINICAL_TRIAL_JOURNAL]
                 }]))

            merged_journals = (
                    (journal_pubmed, journal_clinical_trials)
                    | "Flatten list" >> beam.Flatten())

            merged_drugs_mentioned = ((pubmed, clinical_trials) | beam.CoGroupByKey())

            merged_drugs_mentioned = ((merged_drugs_mentioned, merged_journals) | "groupe by key" >> beam.CoGroupByKey())

            def join_info(name_infos):
                return ({DRUG_NAME: name_infos[0],
                         PUBMED: name_infos[1][0][0][0],
                         CLINICAL_TRIALS: name_infos[1][0][0][1],
                         JOURNALS: name_infos[1][1]
                         })

            merged_drugs_mentioned = merged_drugs_mentioned | beam.Map(join_info)
            merged_drugs_mentioned | beam.Map(print)

            merged_drugs_mentioned | 'JSON formate' >> beam.Map(json.dumps, cls=SetEncoder) \
            | 'Write Output to json' >> beam.io.WriteToText(args.result_file_path, file_name_suffix=".json",
                                                    shard_name_template='')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    PipelineJob().run(argv=sys.argv)
