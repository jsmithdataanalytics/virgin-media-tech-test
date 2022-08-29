import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from task_2 import SumQualifyingTransactionsByDate


class SumQualifyingTransactionsByDateTest(unittest.TestCase):

    @staticmethod
    def test_aggregation_without_filtering():
        """
        test on example data that includes several transactions to be summed for each date
        nothing to be filtered, i.e. all transactions qualify to be included in the sum
        """

        lines = [
            'timestamp,origin,destination,transaction_amount',
            '2017-01-01 04:22:23 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,100.00',
            '2017-01-01 05:22:23 UTC,wallet00000e719adfeaa64b5a,wallet00001e494c12b3083634,200.00',
            '2017-01-01 06:22:23 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,300.00',
            '2018-02-27 16:04:11 UTC,wallet00001866cb7e0f09a890,wallet00000e719adfeaa64b5a,100.00',
            '2018-02-27 17:04:11 UTC,wallet00001e494c12b3083634,wallet00005f83196ec58e4ffe,200.00',
            '2018-02-27 18:04:11 UTC,wallet00005f83196ec58e4ffe,wallet00001866cb7e0f09a890,400.00',
        ]

        with TestPipeline() as pipeline:

            output_pcollection = (
                pipeline
                | beam.Create(lines)
                | 'apply the composite transform being tested'
                >> SumQualifyingTransactionsByDate()
            )

            assert_that(
                output_pcollection,
                equal_to([
                    '{"date": "2018-02-27", "total_amount": "700.00"}',
                    '{"date": "2017-01-01", "total_amount": "600.00"}',
                ])
            )


if __name__ == '__main__':
    unittest.main()
