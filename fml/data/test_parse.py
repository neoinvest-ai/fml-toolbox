# Copyright 2020 Neoinvest.ai
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
