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
from fml.testing.fixtures import BaseTestCase


class TickDataSetTest(BaseTestCase):
    def setUp(self):
        self.test_files = {
            'input_file': self.generate_temp_file(contents=self.TICK_DATA),
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
            assert_that(result, equal_to(self.TICK_DATA_PARSED))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
