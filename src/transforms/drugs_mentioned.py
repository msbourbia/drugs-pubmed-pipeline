import json
import pandas as pd

from src.constants.constants import *
from src.transforms.parse_csv import *


class ClinicalTrialsParseFn(beam.DoFn):
    """Parses the raw of Pubmed into a Python dictionary.
    Each event line has the following format:
      id, scientific_title, date, journal
    """

    def __init__(self):
        beam.DoFn.__init__(self)
        self.num_parse_errors = Metrics.counter(self.__class__, 'num_parse_errors')

    def process(self, elem):
        try:
            row = list(csv.reader([elem]))[0]
            yield {
                CLINICAL_TRIAL_ID: row[0],
                CLINICAL_TRIAL_SCIENTIFIC_TITLE: row[1],
                CLINICAL_TRIAL_DATE: row[2],
                CLINICAL_TRIAL_JOURNAL: row[3],
            }
        except:  # pylint: disable=bare-except
            # Log and count parse errors
            self.num_parse_errors.inc()
            logging.error('Parse error on "%s"', elem)


class clinicalTrialsJournaleCreationFn(beam.DoFn):
    """get journal list of clinical trials
    """

    def __init__(self, drugs_df):
        beam.DoFn.__init__(self)
        self.drugs_df = drugs_df

    def process(self, elem):
        try:
            drugs_cited_df = self.drugs_df.loc[self.drugs_df[DRUG_NAME].isin(elem[CLINICAL_TRIAL_SCIENTIFIC_TITLE].upper().split())]
            drugs_cited_df = drugs_cited_df.reset_index()
            for index, row in drugs_cited_df.iterrows():
                yield (
                    [row[DRUG_NAME],
                     {CLINICAL_TRIAL_SCIENTIFIC_TITLE: elem[CLINICAL_TRIAL_SCIENTIFIC_TITLE],
                      CLINICAL_TRIAL_DATE: elem[CLINICAL_TRIAL_DATE],
                      CLINICAL_TRIAL_JOURNAL: elem[CLINICAL_TRIAL_JOURNAL]
                      }])
        except:  # pylint: disable=bare-except
            # Log and count parse errors
            self.num_parse_errors.inc()
            logging.error('Parse error on "%s"', elem)


class pubmedJournaleCreationFn(beam.DoFn):
    """get journal list of pubmed
    """

    def __init__(self, drugs_df):
        beam.DoFn.__init__(self)
        self.drugs_df = drugs_df

    def process(self, elem):
        try:
            drugs_cited_df = self.drugs_df.loc[self.drugs_df[DRUG_NAME].isin(elem['title'].upper().split())]
            drugs_cited_df = drugs_cited_df.reset_index()  # make sure indexes pair with number of rows
            for index, row in drugs_cited_df.iterrows():
                yield (
                    [row[DRUG_NAME],
                     {PUBMED_TITLE: elem[PUBMED_TITLE],
                      PUBMED_DATE: elem[PUBMED_DATE],
                      PUBMED_JOURNAL: elem[PUBMED_JOURNAL]
                      }])
        except:  # pylint: disable=bare-except
            # Log and count parse errors
            self.num_parse_errors.inc()
            logging.error('Parse error on "%s"', elem)


class PubmedGraph(beam.PTransform):
    """ A transform to get a graph centred(indexed) by drug name with two dimensions :
    pubmed, journals
    """

    def __init__(self, drugs_file_path):
        beam.PTransform.__init__(self)
        self.drugs_df = pd.read_csv(drugs_file_path)

    def expand(self, pcoll):
        return (
                pcoll
                | 'Pubmed parse entries' >> beam.ParDo(PubmedParseFn())
                | 'find drugs cited' >> beam.ParDo(pubmedJournaleCreationFn(self.drugs_df)))


class ClinicalTrialsGraph(beam.PTransform):
    """A transform to get a graph centred(indexed) by drug name with two dimensions :
    clinical trials, journals
    """

    def __init__(self, drugs_file_path):
        beam.PTransform.__init__(self)
        self.drugs_df = pd.read_csv(drugs_file_path)

    def expand(self, pcoll):
        return (
                pcoll
                | 'ClinicalTrials parse entries' >> beam.ParDo(ClinicalTrialsParseFn())
                | 'find drugs cited' >> beam.ParDo(clinicalTrialsJournaleCreationFn(self.drugs_df)))


class SetEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return json.JSONEncoder.default(self, obj)
