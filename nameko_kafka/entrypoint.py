"""
    Nameko kafka entrypoint
"""

import json
import os
from functools import partial

from kafka import KafkaConsumer as Consumer
from nameko.extensions import Entrypoint

from .constants import KAFKA_CONSUMER_CONFIG_KEY


class KafkaConsumer(Entrypoint):
    """
        Kafak consumer extension for Nameko entrypoint.

        :param topics: list of kafka topics to consume
        :param kwargs: additional kafka consumer configurations as
                       keyword arguments
    """

    def __init__(self, *topics, **kwargs):
        self._topics = topics
        self._config = {}
        # Extract kafka config options from keyword arguments
        for option in Consumer.DEFAULT_CONFIG:
            value = kwargs.pop(option, None)
            if value:
                self._config[option] = value
        self._consumer = None
        try:
            super(KafkaConsumer, self).__init__(**kwargs)
        except TypeError:
            raise TypeError("Invalid arguments for Kafka consumer: '{}'".format(kwargs))

    def _parse_config(self):
        cfg = self.container.config.get(KAFKA_CONSUMER_CONFIG_KEY)
        # Check environment variables if config not found in file
        if not cfg:
            _value = os.environ.get(KAFKA_CONSUMER_CONFIG_KEY, "{}")
            cfg = json.loads(_value)
        # Override options from file or env variable with
        # those from keyword arguments.
        for option in self._config:
            cfg[option] = self._config[option]

        return cfg

    def setup(self):
        config = self._parse_config()
        self._consumer = Consumer(*self._topics, **config)

    def start(self):
        self.container.spawn_managed_thread(
            self.run, identifier="{}.run".find(self.__class__.__name__)
        )

    def stop(self):
        self._consumer.close()

    def run(self):
        for message in self._consumer:
            self.handle_message(message)  # pragma: no cover

    def handle_message(self, message):
        handle_result = partial(self.handle_result, message)

        args = (message,)
        kwargs = {}

        self.container.spawn_worker(
            self, args, kwargs, handle_result=handle_result
        )

    def handle_result(self, message, worker_ctx, result, exc_info):
        # TODO: Add commit storage for exactly-once delivery
        return result, exc_info


consume = KafkaConsumer.decorator
