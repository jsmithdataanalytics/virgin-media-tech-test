import json

from apache_beam import Pipeline, Filter, Map, MapTuple, PTransform, CombinePerKey
from apache_beam.io import ReadFromText, WriteToText

from task_1 import parse_csv_line


class SumQualifyingTransactionsByDate(PTransform):

    # noinspection PyTypeChecker
    def expand(self, pcoll):
        return (
            pcoll
            | 'drop header line'
            >> Filter(lambda line: not line.startswith('timestamp'))
            | 'get date and amount'
            >> Map(parse_csv_line)
            | 'filter for transaction_amount greater than 20 and timestamp in or after 2010'
            >> Filter(lambda item: item[0] >= '2010-01-01' and item[1] > 20)
            | 'sum transaction amounts by date'
            >> CombinePerKey(sum)
            | 'reformat as json objects'
            >> MapTuple(lambda date, amount: json.dumps({'date': date, 'total_amount': '{:.2f}'.format(amount)}))
        )


with Pipeline() as pipeline:
    (
        pipeline
        | 'read input file lines'
        >> ReadFromText(file_pattern='gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv')
        | 'transform into json output'
        >> SumQualifyingTransactionsByDate()
        | 'write output file'
        >> WriteToText(
            file_path_prefix='output/results',
            file_name_suffix='.jsonl.gz',
            compression_type='gzip',
            num_shards=1,
            shard_name_template='',
        )
    )
