import datetime as dt
import csv
import logging

import apache_beam as beam
from apache_beam.metrics.metric import Metrics


class TickBarFn(beam.DoFn):
    """
    Parse the tick objects into Tick Bars
    """

    def __init__(self, threshold=10):
        """
        Tick Bar Function
        :param threshold: The number of ticks at which we extract the bid ask
        :type threshold: int
        """
        beam.DoFn.__init__(self)  # BEAM-6158
        self.number = Metrics.counter(self.__class__, 'ticks_processed')
        self.threshold = threshold

    def process(self, element):
        self.number.inc()
        if self.number == self.threshold:
            self.number = Metrics.counter(self.__class__,
                                          'ticks_processed')
            yield element


class VolumeBarFn(beam.DoFn):
    """
    Parse the tick objects into Tick Bars
    """

    def __init__(self):
        beam.DoFn.__init__(self)  # BEAM-6158
        self.errors_parse_num = Metrics.counter(self.__class__,
                                                'errors_parse_num')

    def process(self, element):
        try:
            row = list(csv.reader([element]))[0]
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


class TickBar(beam.PTransform):
    def expand(self, pcoll):
        return (
                pcoll
                | 'TickBarFn' >> beam.ParDo(TickBarFn())
        )


class VolumeBar(beam.PTransform):
    def expand(self, pcoll):
        return (
                pcoll
                | 'VolumeBar' >> beam.ParDo(VolumeBarFn())
        )
