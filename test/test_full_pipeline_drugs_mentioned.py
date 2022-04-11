import json
import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, is_empty
from apache_beam.testing.util import equal_to

from src.jobs.pipeline_drugs_mentioned import PipelineJob
from src.transforms.drugs_mentioned import *


def ordered(obj):
    if isinstance(obj, dict):
        return sorted((k, ordered(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return sorted(ordered(x) for x in obj)
    else:
        return obj


class DataPreparationTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        cls.pipeline_args = [
            '--runner=DirectRunner',
            '--project=xxxx',
            '--region=europe-west1',
            '--staging_location=/tmp/',
            '--temp_location=/tmp/',
            '--job_name=data-prep-job-integ-test',
            '--network=default',
            '--subnetwork=regions/europe-west1/subnetworks/default',
            '--drugs_file_path=resources/drugs.csv',
            '--pubmed_file_path=resources/pubmed.csv',
            '--clinical_trials_file_path=resources/clinical_trials.csv',
            '--result_file_path=resources/result_drug_mentioned'
        ]
        print("setUpClass")

    def test_full_pipeline_data_preparation(self):
        # GIVEN
        class DataPreparationJob:
            def __init__(_self, pipeline_args):
                _self._pipeline_args = pipeline_args

            def run(_self, argv=None, save_main_session=True):
                    PipelineJob().run(argv=_self._pipeline_args)
                    print("end pipeline")

        # WHEN
        DataPreparationJob(self.pipeline_args).run()

        # THEN
        with open("test/resources/expected_result_drug_mentioned.json") as f_expected:
            with open("test/resources/result_drug_mentioned.json") as f:
                expected_lines = f_expected.readlines()
                result_lines = f.readlines()

                expected_data = {"data": expected_lines}
                result_data = {"data": result_lines}

                expected_json = json.loads(json.dumps(expected_data, sort_keys=True))
                result_json = json.loads(json.dumps(result_data, sort_keys=True))

                self.assertEqual(True, expected_json == result_json)


class TestValidateData(unittest.TestCase):

    def test_valid_pubmed(self):
        line_str = '3,"Diphenhydramine hydrochloride helps symptoms of ciguatera fish poisoning.",02/01/2019,"The Journal of pediatrics"'
        expected_result = [{'id': '3', 'title': 'Diphenhydramine hydrochloride helps symptoms of ciguatera fish poisoning.', 'date': '02/01/2019', 'journal': 'The Journal of pediatrics'}]
        with TestPipeline() as p:
            pcoll = (
                    p
                    | beam.Create([line_str])
                    | 'Pubmed parse entries' >> beam.ParDo(PubmedParseFn()))

            assert_that(pcoll,
                        equal_to(expected_result))

    def test_not_valid_pubmed(self):
        line_str = '3,'
        with TestPipeline() as p:
            pcoll = (
                    p
                    | beam.Create([line_str])
                    | 'Pubmed parse entries' >> beam.ParDo(PubmedParseFn()))

            assert_that(pcoll,
                        is_empty())

    def test_not_valid_pubmed_2(self):
        line_str = {
                     "id": 9,
                     "title": "Gold nanoparticles synthesized from Euphorbia fischeriana root by green route method alleviates the isoprenaline hydrochloride induced myocardial infarction in rats.",
                     "date": "01/01/2020",
                     "journal": "Journal of photochemistry and photobiology. B, Biology"
                   }
        with TestPipeline() as p:
            pcoll = (
                    p
                    | beam.Create([line_str])
                    | 'Pubmed parse entries' >> beam.ParDo(PubmedParseFn()))

            assert_that(pcoll,
                        is_empty())

    def test_valid_clinical_trial(self):
        line_str = 'NCT04237091,"Feasibility of a Randomized Controlled Clinical Trial Comparing the Use of Cetirizine to Replace Diphenhydramine in the Prevention of Reactions Related to Paclitaxel","1 January 2020","Journal of emergency nursing"'
        expected_result = [{'id': 'NCT04237091', 'scientific_title': 'Feasibility of a Randomized Controlled Clinical Trial Comparing the Use of Cetirizine to Replace Diphenhydramine in the Prevention of Reactions Related to Paclitaxel', 'date': '1 January 2020', 'journal': 'Journal of emergency nursing'}]
        with TestPipeline() as p:
            pcoll = (
                    p
                    | beam.Create([line_str])
                    | 'Clinical Trials parse entries' >> beam.ParDo(ClinicalTrialsParseFn()))

            assert_that(pcoll,
                        equal_to(expected_result))

    def test_not_valid_clinical_trial(self):
        line_str = ''
        with TestPipeline() as p:
            pcoll = (
                    p
                    | beam.Create([line_str])
                    | 'ClinicalTrials parse entries' >> beam.ParDo(ClinicalTrialsParseFn()))

            assert_that(pcoll,
                        is_empty())

    def test_not_valid_clinical_trial_2(self):
        line_str = 'aaaa,bbbb'
        with TestPipeline() as p:
            pcoll = (
                    p
                    | beam.Create([line_str])
                    | 'ClinicalTrials parse entries' >> beam.ParDo(ClinicalTrialsParseFn()))

            assert_that(pcoll,
                        is_empty())





if __name__ == "__main__":
    unittest.main()
