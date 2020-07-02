import os
import random
import datetime as dt
from collections import namedtuple
import tempfile
import unittest

Quote = namedtuple('Quote', ('price', 'bid', 'ask'))
Tick = namedtuple('Tick', ('time', 'price', 'bid', 'ask'))


class TickFactory:
    def __init__(self, tick_number=10, price_average=100, price_volatility=0.1,
                 start_time=dt.datetime.now() - dt.timedelta(minutes=10),
                 end_time=dt.datetime.now(), store_ticks=False):
        """
        Tick factory
        :param tick_number:
        :type tick_number:
        :param price_average:
        :type price_average:
        :param price_volatility:
        :type price_volatility:
        :param start_time:
        :type start_time:
        :param end_time:
        :type end_time:
        :param store_ticks:
        :type store_ticks:
        """
        self.tick_number = tick_number
        self.price_average = price_average
        self.price_volatility = price_volatility
        self.price_now = price_average
        self.time_now = start_time
        self.time_step = (end_time - start_time) / tick_number
        self.time_delta = None
        self.store_ticks = store_ticks
        self.ticks = []

    def random_step(self, start, delta):
        """
        This function returns a random time between start and start + delta
        :param start: start time
        :type start: dt.datetime
        :param delta: time delta
        :type delta: dt.timedelta
        :return: random step
        :rtype: dt.datetime
        """
        self.time_delta = dt.timedelta(
            seconds=random.uniform(0, delta.total_seconds())
        )
        return start + self.time_delta

    def random_quote(self, price_current, delta, volatility):
        """

        :param price_current: Current price that will be used to calculate
        the new quote
        :type price_current: float
        :param delta: Time delta
        :type delta:
        :param volatility:
        :type volatility:
        :return:
        :rtype:
        """
        price = price_current + \
                random.choice(
                    (-1, 1)) * delta.total_seconds() ** 0.5 * volatility
        return Quote(
            price=price,
            bid=price - 0.1 * random.random(),
            ask=price + 0.1 * random.random()
        )

    def generate_one_tick(self):
        """

        :return: A tick in string format e.g
        "06/19/2020,16:00:00,109.34,109.32,109.38,379\n"
        :rtype: str
        """
        tick_time = self.random_step(start=self.time_now,
                                     delta=self.time_step)
        quote = self.random_quote(
            price_current=self.price_now,
            delta=self.time_delta,
            volatility=self.price_volatility
        )
        tick = Tick(
            time=tick_time,
            price=quote.price,
            bid=quote.bid,
            ask=quote.ask
        )
        if self.store_ticks:
            self.ticks.append(tick)

        self.price_now = tick.price
        self.time_now = tick.time
        return (f"{tick.time.strftime('%m/%d/%Y,%H:%M:%S')},"
                f"{tick.price},{tick.bid},{tick.ask}")

    def generate_all_ticks(self):
        """
        Generator function to generate multiple ticks (total equal to
        tick_number)
        :return: yields a generated tick
        :rtype: str
        """
        for _ in range(self.tick_number):
            yield self.generate_one_tick()

    def to_file(self, output_file):
        with open(output_file, 'w') as o:
            for tick in self.generate_all_ticks():
                o.write(tick)


class BaseTestCase(unittest.TestCase):
    def setUpClass(cls) -> None:
        cls.ticks_factory = TickFactory()

    def setUp(self):
        self.test_files = {
            'input_file': self.generate_temp_file(),
            'output_file': self.generate_temp_file()
        }

    def tearDown(self):
        for test_file in self.test_files.values():
            if os.path.exists(test_file):
                os.remove(test_file)

    def generate_temp_file(self, contents=None):
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            if contents is not None:
                for content in contents:
                    temp_file.write(content.encode('utf-8'))
            return temp_file.name
