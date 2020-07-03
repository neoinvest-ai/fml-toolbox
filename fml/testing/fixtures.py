import os
import random
import datetime as dt
from collections import namedtuple
import tempfile
import unittest

from fml.data.models import Quote, Tick


class TickFactory:
    def __init__(self, tick_number=10, price_average=100, price_volatility=0.1,
                 start_time=dt.datetime.now() - dt.timedelta(minutes=10),
                 end_time=dt.datetime.now(), store_ticks=False):
        """
        Tick factory
        :param tick_number: Number of ticks to generate
        :type tick_number: int
        :param price_average: Average price
        :type price_average: float
        :param price_volatility: Average volatility
        :type price_volatility: float
        :param start_time: Start time defaulted to now - 10 minutes
        :type start_time: dt.datetime
        :param end_time: End time defaulted to now
        :type end_time: dt.datetime
        :param store_ticks: option to store or not ticks in the self instance
        :type store_ticks: bool
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
            ask=quote.ask,
            quantity=random.uniform(100, 200)
        )
        if self.store_ticks:
            self.ticks.append(tick)

        self.price_now = tick.price
        self.time_now = tick.time
        return (f"{tick.time.strftime('%m/%d/%Y,%H:%M:%S')},"
                f"{tick.price},{tick.bid},{tick.ask},{tick.quantity}")

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
    TICK_DATA = (
        "06/19/2020,16:00:00,109.34,109.32,109.38,500\n"
        "06/19/2020,16:03:13,109.37,109.37,112.66,1700\n"
        "06/19/2020,16:03:13,109.37,109.37,112.66,750\n"
        "06/19/2020,16:03:13,109.37,109.37,112.66,250\n"
        "wrong_line\n"
        "06/19/2020,16:03:13,109.37,109.37,112.66,1000\n"
        "06/19/2020,16:03:13,109.37,109.37,112.66,750\n"
        "06/19/2020,16:03:14,109.37,109.37,110.54,500"
    )

    TICK_DATA_PARSED = [
        Tick(time=dt.datetime(2020, 6, 19, 16, 0),
             price=109.34, bid=109.32, ask=109.38, quantity=500),
        Tick(time=dt.datetime(2020, 6, 19, 16, 3, 13),
             price=109.37, bid=109.37, ask=112.66, quantity=1700),
        Tick(time=dt.datetime(2020, 6, 19, 16, 3, 13),
             price=109.37, bid=109.37, ask=112.66, quantity=750),
        Tick(time=dt.datetime(2020, 6, 19, 16, 3, 13),
             price=109.37, bid=109.37, ask=112.66, quantity=250),
        Tick(time=dt.datetime(2020, 6, 19, 16, 3, 13),
             price=109.37, bid=109.37, ask=112.66, quantity=1000),
        Tick(time=dt.datetime(2020, 6, 19, 16, 3, 13),
             price=109.37, bid=109.37, ask=112.66, quantity=750),
        Tick(time=dt.datetime(2020, 6, 19, 16, 3, 14),
             price=109.37, bid=109.37, ask=110.54, quantity=500)
    ]

    @classmethod
    def setUpClass(cls) -> None:
        cls.ticks_factory = TickFactory()

    def setUp(self):
        self.test_files = {
            'input_file': self.generate_temp_file(
                contents=self.ticks_factory.generate_all_ticks()
            ),
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
