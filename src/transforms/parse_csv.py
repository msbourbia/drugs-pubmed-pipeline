import csv
import logging

import apache_beam as beam
from apache_beam.metrics.metric import Metrics

from src.constants.constants import *


class DrugsPerseFn(beam.DoFn):
    """Parses the raw of drug into a Python dictionary.
    Each event line has the following format:
      atccode, drug
    """

    def __init__(self):
        beam.DoFn.__init__(self)
        self.num_parse_errors = Metrics.counter(self.__class__, 'num_parse_errors')

    def process(self, elem):
        try:
            row = list(csv.reader([elem]))[0]
            yield {
                DRUG_ATCCODE: row[0],
                DRUG_NAME: row[1],
            }
        except:  # pylint: disable=bare-except
            # Log and count parse errors
            self.num_parse_errors.inc()
            logging.error('Parse error on "%s"', elem)


class PubmedParseFn(beam.DoFn):
    """Parses the raw of Pubmed into a Python dictionary.
    Each event line has the following format:
      id, title, date, journal
    """

    def __init__(self):
        beam.DoFn.__init__(self)
        self.num_parse_errors = Metrics.counter(self.__class__, 'num_parse_errors')

    def process(self, elem):
        try:
            row = list(csv.reader([elem]))[0]
            yield {
                PUBMED_ID: row[0],
                PUBMED_TITLE: row[1],
                PUBMED_DATE: row[2],
                PUBMED_JOURNAL: row[3],
            }
        except:  # pylint: disable=bare-except
            # Log and count parse errors
            self.num_parse_errors.inc()
            logging.error('Parse error on "%s"', elem)
