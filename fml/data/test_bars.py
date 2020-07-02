import logging
import datetime
import os
import tempfile
import unittest

from fml import pipeline


class TickDataSetTest(unittest.TestCase):
    SAMPLE_DATA = [
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
                  ],

    def setUp(self):
        self.test_files = {
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


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
