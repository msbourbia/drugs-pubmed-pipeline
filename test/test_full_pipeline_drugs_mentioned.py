import json
import unittest

from src.jobs.pipeline_drugs_mentioned import PipelineJob


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


if __name__ == "__main__":
    unittest.main()
