import logging
import os
import tempfile
import unittest
import datetime

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from fml.data.parse import ParseTick


class TickDataSetTest(unittest.TestCase):
    SAMPLE_DATA = (
        "06/19/2020,16:00:00,109.34,109.32,109.38,379\n"
        "06/19/2020,16:03:13,109.37,109.37,112.66,1700\n"
        "06/19/2020,16:03:13,109.37,109.37,112.66,750\n"
        "06/19/2020,16:03:13,109.37,109.37,112.66,250\n"
        "wrong_line\n"
        "06/19/2020,16:03:13,109.37,109.37,112.66,1000\n"
        "06/19/2020,16:03:13,109.37,109.37,112.66,750\n"
        "06/19/2020,16:03:14,109.37,109.37,110.54,500"
    )

    PARSED_TICK_DATA = [
        {'time': datetime.datetime(2020, 6, 19, 16, 0), 'price':
            109.34, 'bid': 109.32, 'ask': 109.38},
        {'time': datetime.datetime(2020, 6, 19, 16, 3, 13),
         'price': 109.37, 'bid': 109.37, 'ask': 112.66},
        {'time': datetime.datetime(2020, 6, 19, 16, 3, 13),
         'price': 109.37, 'bid': 109.37, 'ask': 112.66},
        {'time': datetime.datetime(2020, 6, 19, 16, 3, 13),
         'price': 109.37, 'bid': 109.37, 'ask': 112.66},
        {'time': datetime.datetime(2020, 6, 19, 16, 3, 13),
         'price': 109.37, 'bid': 109.37, 'ask': 112.66},
        {'time': datetime.datetime(2020, 6, 19, 16, 3, 13),
         'price': 109.37, 'bid': 109.37, 'ask': 112.66},
        {'time': datetime.datetime(2020, 6, 19, 16, 3, 14),
         'price': 109.37, 'bid': 109.37, 'ask': 110.54}
    ]

    def setUp(self):
        self.test_files = {
            'input_file': self.generate_temp_file(contents=self.SAMPLE_DATA),
            'output_file': self.generate_temp_file()
        }

    def tearDown(self):
        for test_file in self.test_files.values():
            if os.path.exists(test_file):
                os.remove(test_file)

    def generate_temp_file(self, contents=None):
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            if contents is not None:
                temp_file.write(contents.encode('utf-8'))
            return temp_file.name

    def test_output_file(self):
        with TestPipeline() as p:  # Use TestPipeline for testing.
            result = (
                    p | 'ReadInputFile' >> beam.io.ReadFromText(
                        self.test_files['input_file'])
                      | 'ParseTickData' >> ParseTick()
            )
            assert_that(result, equal_to(self.PARSED_TICK_DATA))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
