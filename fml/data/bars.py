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
        self.ticks_processed = Metrics.counter(self.__class__,
                                               'ticks_processed')
        self.buffer = 0
        self.threshold = threshold

    def process(self, element):
        self.buffer += 1
        self.ticks_processed.inc()
        if self.buffer == self.threshold:
            self.buffer = 0
            yield element


class VolumeBarFn(beam.DoFn):
    """
    Parse the tick objects into volume bars
    """

    def __init__(self, threshold=10000):
        """
        Volume Bar Function
        :param threshold: The accumulated volume threshold at which we extract
        the bid ask
        :type threshold: float
        """
        beam.DoFn.__init__(self)  # BEAM-6158
        self.ticks_processed = Metrics.counter(self.__class__,
                                               'ticks_processed')
        self.buffer = 0
        self.threshold = threshold

    def process(self, element):
        self.buffer += element.quantity * element.price
        self.ticks_processed.inc()
        if self.buffer >= self.threshold:
            self.buffer = 0
            yield element


class TickBar(beam.PTransform):
    def __init__(self, threshold=10):
        self.threshold = threshold
        super().__init__()

    def expand(self, pcoll):
        return (
                pcoll
                | 'TickBarFn' >> beam.ParDo(
                    TickBarFn(threshold=self.threshold)
                )
        )


class VolumeBar(beam.PTransform):
    def __init__(self, threshold=1000):
        self.threshold = threshold
        super().__init__()

    def expand(self, pcoll):
        return (
                pcoll
                | 'VolumeBar' >> beam.ParDo(
                    VolumeBarFn(threshold=self.threshold)
                )
        )
