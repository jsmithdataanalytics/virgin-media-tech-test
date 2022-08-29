import json
from typing import Tuple

import pytz
from apache_beam import Pipeline, Filter, Map, MapTuple, CombinePerKey
from apache_beam.io import ReadFromText, WriteToText
from dateutil.parser import parse as parse_timestamp


def parse_csv_line(line: str) -> Tuple[str, float]:
    """get utc date and transaction amount from csv line"""
    value_strings = line.split(',')
    utc_datetime = parse_timestamp(value_strings[0]).astimezone(pytz.utc)
    utc_date = utc_datetime.date()
    transaction_amount = float(value_strings[-1])

    return str(utc_date), transaction_amount


with Pipeline() as pipeline:
    # noinspection PyTypeChecker
    (
        pipeline
        | 'read input file lines'
        >> ReadFromText(file_pattern='gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv')
        | 'drop header line'
        >> Filter(lambda line: not line.startswith('timestamp'))
        | 'get date and amount'
        >> Map(parse_csv_line)
        | 'filter for transaction_amount greater than 20 and timestamp in or after 2010'
        >> Filter(lambda item: item[0] >= '2010-01-01' and item[1] > 20)
        | 'sum transaction amounts by date'
        >> CombinePerKey(sum)
        | 'reformat as json objects'
        >> MapTuple(lambda date, amount: json.dumps({'date': date, 'total_amount': amount}))
        | 'write output file'
        >> WriteToText(
            file_path_prefix='output/results',
            file_name_suffix='.jsonl.gz',
            compression_type='gzip',
            num_shards=1,
            shard_name_template='',
        )
    )
