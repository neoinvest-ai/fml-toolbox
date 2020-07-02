import unittest
import datetime as dt

import fml.testing.fixtures


class TickFactoryTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tick_factory = fml.testing.fixtures.TickFactory()

    def test_random_quote(self):
        quote = self.tick_factory.random_quote(
            price_current=100, delta=dt.timedelta(seconds=0), volatility=0
        )
        self.assertEqual(quote.price, 100)

    def test_random_step(self):
        # Testing with limit conditions (delta = 0)
        start = dt.datetime.now()
        delta = dt.timedelta(seconds=0)
        step = self.tick_factory.random_step(start=start, delta=delta)
        self.assertEqual(start, step)

        # Testing with normal conditions (delta = 10)
        delta = dt.timedelta(seconds=10)
        step = self.tick_factory.random_step(start=start, delta=delta)
        self.assertLessEqual(step - start, delta)

    def test_generate_one_tick(self):
        tick = self.tick_factory.generate_one_tick()
        self.assertNotEqual(tick, "")

    def test_generate_multiple_ticks(self):
        ticks = self.tick_factory.generate_all_ticks()
        for tick in ticks:
            self.assertNotEqual(tick, "")


if __name__ == '__main__':
    unittest.main()
