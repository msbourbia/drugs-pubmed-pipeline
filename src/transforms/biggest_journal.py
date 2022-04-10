import json

import apache_beam as beam


class JouranalListFn(beam.DoFn):
    """Parses the raw and
    for each line return a list of tuple (journal, drug)
    """

    def __init__(self):
        beam.DoFn.__init__(self)

    def process(self, elem):
        journal_list = [(journal['journal'], elem['drug']) for journal in elem['journals']]
        yield journal_list


class FoundBiggestJournal(beam.PTransform):
    """A transform to extract a 'biggest' journal : who mentioned a biggest number of drugs
    """

    def __init__(self):
        beam.PTransform.__init__(self)

    @staticmethod
    def generate_elements(elements):
        for element in elements:
            yield element

    @staticmethod
    def sort_grouped_data(row):
        (key_number, sort_data) = row
        sort_data.sort(key=lambda x: x[1], reverse=True)
        return sort_data[0][0]

    @staticmethod
    def add_key(row):
        return (1, row)

    @staticmethod
    def droug_list_size(row):
        "count the number of drugs mentioned by jouranl"
        return row[0], len(row[1])

    def expand(self, pcoll):
        return (pcoll
                       | 'Format entry to get list of (journal, drug)' >> beam.ParDo(JouranalListFn()) \
                       | 'Flat lists to pcoll of tuple (journal, drug)' >> beam.FlatMap(self.generate_elements)) \
                       | 'Deduplicate elements' >> beam.Distinct() \
                       | 'Group by journal' >> beam.GroupByKey() \
                       | 'Count drugs list size by journal' >> beam.Map(self.droug_list_size) \
                       | 'Add key' >> beam.Map(self.add_key) \
                       | 'Group by key' >> beam.GroupByKey() \
                       | 'Sort grouped data and return top' >> beam.Map(self.sort_grouped_data)


class SetEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return json.JSONEncoder.default(self, obj)


class JsonCoder(object):
    """A JSON coder interpreting each line as a JSON string."""

    @staticmethod
    def encode(x):
        return json.dumps(x)

    @staticmethod
    def decode(x):
        return json.loads(x)
