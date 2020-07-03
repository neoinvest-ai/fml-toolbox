import logging
import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from fml.testing.fixtures import BaseTestCase, TickFactory
from fml.data import bars


class TickDataSetTest(BaseTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.ticks_factory = TickFactory(
            tick_number=100,
            store_ticks=True
        )

    def test_bar_data(self):

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


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
