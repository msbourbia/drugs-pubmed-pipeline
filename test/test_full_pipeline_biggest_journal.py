import unittest



from src.jobs.pipeline_biggest_journal import PipelineJob



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
            '--json_file_path=test/resources/result_drug_mentioned.json',
            '--result_file_path=test/resources/result_biggest_journal'
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
        with open('test/resources/result_biggest_journal.txt') as f:
            result = f.readlines()
            self.assertEqual(result, ['Psychopharmacology\n'])





if __name__ == "__main__":
    unittest.main()
