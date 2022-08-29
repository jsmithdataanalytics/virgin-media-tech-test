import sys
import unittest

from task_2 import create_pipeline
from test import SumQualifyingTransactionsByDateTest

# run unit test
print('\nRunning unit test...')
suite = unittest.TestSuite()
suite.addTest(SumQualifyingTransactionsByDateTest("test_aggregation_without_filtering"))
runner = unittest.TextTestRunner(stream=sys.__stdout__)
result = runner.run(suite)

# run pipeline
print('\nRunning pipeline on the real file...')
create_pipeline().run().wait_until_finish()
print('\nComplete!')
