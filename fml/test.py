import logging
import os
import tempfile
import unittest

from fml import pipeline


class TickDataSetTest(unittest.TestCase):
    SAMPLE_DATA = (
        "06/19/2020,16:00:00,109.34,109.32,109.38,379\n"
        "06/19/2020,16:03:13,109.37,109.37,112.66,1700\n"
        "06/19/2020,16:03:13,109.37,109.37,112.66,750\n"
        "06/19/2020,16:03:13,109.37,109.37,112.66,250\n"
        "06/19/2020,16:03:13,109.37,109.37,112.66,1000\n"
        "06/19/2020,16:03:13,109.37,109.37,112.66,750\n"
        "06/19/2020,16:03:14,109.37,109.37,110.54,500"
    )

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

    def run_example(self):
        args = [
            '--input=%s' % self.test_files[
                'input_file'],
            '--output=%s' % self.test_files[
                'output_file'],
        ]

        pipeline.run(args)

    def test_output_file(self):
        self.run_example()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
