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
import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from fml.testing.fixtures import BaseTestCase, TickFactory
from fml.data import bars


class TickBarTestCase(BaseTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.ticks_factory = TickFactory(
            tick_number=100,
            store_ticks=True
        )

    def test_tick_bar_data(self):

        expected_ticks = []
        count = 1
        for tick in self.ticks_factory.ticks:
            if count % 10 == 0:
                expected_ticks.append(tick)
            count += 1
        with TestPipeline() as p:  # Use TestPipeline for testing.
            result = (
                    p | beam.Create(self.ticks_factory.ticks)
                    | bars.TickBar(threshold=10)
            )
            assert_that(result, equal_to(expected_ticks))


class VolumeBarTestCase(BaseTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.ticks_factory = TickFactory(
            tick_number=2,
            store_ticks=True
        )

    def test_volume_bar_data(self):
        expected_ticks = []
        volume_threshold = 100000
        volume = 0
        for tick in self.ticks_factory.ticks:
            volume += tick.price * tick.quantity
            if volume >= volume_threshold:
                expected_ticks.append(tick)
                volume = 0
        logging.info(f"Volume threshold = {volume_threshold}")
        logging.info(expected_ticks)
        with TestPipeline() as p:  # Use TestPipeline for testing.
            result = (
                    p | beam.Create(self.ticks_factory.ticks)
                    | bars.VolumeBar(threshold=volume_threshold)
            )
            assert_that(result, equal_to(expected_ticks))

    def test_sample_volume_bar_data(self):
        with TestPipeline() as p:  # Use TestPipeline for testing.
            result = (
                    p | beam.Create(self.TICK_DATA_PARSED)
                    | bars.VolumeBar(threshold=100000)
            )
            assert_that(result, equal_to(
                [self.TICK_DATA_PARSED[1], self.TICK_DATA_PARSED[3],
                 self.TICK_DATA_PARSED[4], self.TICK_DATA_PARSED[6]]
            ))



if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
