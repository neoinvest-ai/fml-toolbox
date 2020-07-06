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

import datetime as dt
import csv
import logging

import apache_beam as beam
from apache_beam.metrics.metric import Metrics

from fml.data.models import Tick


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
            yield Tick(
                time=dt.datetime.strptime(
                    f"{row[0]},{row[1]}",
                    '%m/%d/%Y,%H:%M:%S'
                ),
                price=float(row[2]),
                bid=float(row[3]),
                ask=float(row[4]),
                quantity=float(row[5])
            )
        except:
            self.errors_parse_num.inc()
            logging.error(f"Parsing error of element = {element}")


class ParseTick(beam.PTransform):
    def expand(self, pcoll):
        return (
                pcoll
                | 'ParseTickDataFn' >> beam.ParDo(ParseTickDataFn())
        )
