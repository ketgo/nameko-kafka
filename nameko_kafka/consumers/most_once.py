"""
    At most once semantic Kafka message consumer
"""

import time

from .default import DefaultConsumer


class AtMostOnceConsumer(DefaultConsumer):
    """
        At most once semantic Kafka message consumer.

        :param topics: list of kafka topics to consume
        :param kwargs: additional kafka consumer configurations as
            keyword arguments
    """

    CONFIG = {
        # Setting auto commit off as we will manually perform offset commits
        "enable_auto_commit": False
    }

    def __init__(self, *topics, **kwargs):
        # Override passed consumer config with correct parameters
        for option, value in self.CONFIG.items():
            kwargs[option] = value
        super(AtMostOnceConsumer, self).__init__(*topics, **kwargs)
        self._generator = None

    def __next__(self):
        if self._closed:
            raise StopIteration('KafkaConsumer closed')  # pragma: no cover
        return self._next()

    def _next(self):
        """
            Get next message from the polled batch.
        """
        # Sets the value of self._consumer_timeout to current time added with
        # consumer_timeout_ms value specified in the configurations.
        self._set_consumer_timeout()
        while time.time() < self._consumer_timeout:
            if not self._generator:
                self._generator = self._message_generator()
            try:
                return next(self._generator)
            except StopIteration:
                self._generator = None
        raise StopIteration()  # pragma: no cover

    def _message_generator(self):
        """
            Generator yielding messages polled from broker. The offsets
            are committed before yielding the messages resulting in at
            most once delivery.
        """
        timeout_ms = 1000 * (self._consumer_timeout - time.time())
        messages = self.poll(timeout_ms=timeout_ms)

        # Commits the latest offsets returned by poll
        self.commit()

        for topic, partition in messages.items():
            for message in partition:
                yield message
