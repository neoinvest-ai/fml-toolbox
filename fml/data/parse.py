import datetime as dt
import csv
import logging

import apache_beam as beam
from apache_beam.metrics.metric import Metrics


class ParseTickDataFn(beam.DoFn):
    """
    Parse the raw tick data events into a tick object
    """

    def __init__(self):
        beam.DoFn.__init__(self)  # BEAM-6158
        self.errors_parse_num = Metrics.counter(self.__class__,
                                                'errors_parse_num')
        self.ticks_counter = Metrics.counter(self.__class__, 'ticks')

    def process(self, element):
        try:
            row = list(csv.reader([element]))[0]
            self.ticks_counter.inc()
            yield {
                'time': dt.datetime.strptime(
                    f"{row[0]},{row[1]}",
                    '%m/%d/%Y,%H:%M:%S'
                ),
                'price': float(row[2]),
                'bid': float(row[3]),
                'ask': float(row[4])
            }
        except:
            self.errors_parse_num.inc()
            logging.error(f"Parsing error of {element}")


class ParseTick(beam.PTransform):
    def expand(self, pcoll):
        return (
                pcoll
                | 'ParseTickDataFn' >> beam.ParDo(ParseTickDataFn())
        )
