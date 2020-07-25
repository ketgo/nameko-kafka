"""
    Exactly once semantic Kafka message consumer
"""

import time

from kafka import ConsumerRebalanceListener

from .default import DefaultConsumer
from ..storage.base import OffsetStorage


class SaveOffsetsRebalanceListener(ConsumerRebalanceListener):
    """
        Class to store offsets on re-balancing of Kafka consumer
        groups.

        :param consumer: kafka consumer instance
        :param storage: offset storage instance
    """

    def __init__(self, consumer, storage):
        self._consumer = consumer
        self._storage = storage

    def on_partitions_revoked(self, revoked):
        self._storage.commit()

    def on_partitions_assigned(self, assigned):
        for tp in assigned:
            offset = self._storage.read(tp.topic, tp.partition)
            self._consumer.seek(tp, offset)


class ExactlyOnceConsumer(DefaultConsumer):
    """
        Exactly once semantic Kafka message consumer.

        :param topics: list of kafka topics to consume
        :param storage: offset storage instance
        :param kwargs: additional kafka consumer configurations as
            keyword arguments
    """

    CONFIG = {
        # Setting auto commit off as we will manually perform offset commits
        "enable_auto_commit": False
    }

    def __init__(self, *topics, storage: OffsetStorage, **kwargs):
        # Override passed consumer config with correct parameters
        for option, value in self.CONFIG.items():
            kwargs[option] = value
        super(ExactlyOnceConsumer, self).__init__(*topics, **kwargs)
        self._storage = storage
        self._storage.setup()
        self._generator = None

    def close(self, autocommit=True):
        self._storage.stop()
        super(ExactlyOnceConsumer, self).close(autocommit)

    def __iter__(self):
        # Subscribe to the topic we want to consume
        topics = list(self.subscription())
        listener = SaveOffsetsRebalanceListener(self, self._storage)
        self.subscribe(topics, listener=listener)
        # Poll once to ensure joining of consumer group and partitions assigned
        self.poll(0)

        # Seek to the offsets stored in the storage
        for tp in self.assignment():
            offset = self._storage.read(tp.topic, tp.partition)
            self.seek(tp, offset)

        return self

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
            are committed after yielding each message resulting in exactly
            once delivery.
        """
        timeout_ms = 1000 * (self._consumer_timeout - time.time())
        messages = self.poll(timeout_ms=timeout_ms)

        with self._storage as storage:
            for topic, partition in messages.items():
                for message in partition:
                    yield message
                    # Add message offset to storage
                    storage.add(message.topic, message.partition, message.offset)
