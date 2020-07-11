"""
    Nameko kafka entrypoint
"""

import json
import os
from enum import Enum

from nameko.extensions import Entrypoint

from .constants import KAFKA_CONSUMER_CONFIG_KEY
from .consumers import (BaseConsumer, DefaultKafkaConsumer, AtLeastOnceConsumer,
                        AtMostOnceConsumer)


class Semantic(Enum):
    """
        Supported message delivery semantics
    """

    DEFAULT = "DEFAULT"
    AT_LEAST_ONCE = "AT_LEAST_ONCE"
    AT_MOST_ONCE = "AT_MOST_ONCE"
    EXACTLY_ONCE = "EXACTLY_ONCE"


# Consumer factory
CONSUMER_FACTORY = {
    Semantic.DEFAULT: DefaultKafkaConsumer,
    Semantic.AT_LEAST_ONCE: AtLeastOnceConsumer,
    Semantic.AT_MOST_ONCE: AtMostOnceConsumer,
}


class KafkaConsumer(Entrypoint):
    """
        Kafak consumer extension for Nameko entrypoint.

        :param topics: list of kafka topics to consume
        :param semantic: message delivery semantic to be used by
            kafka consumer. Possible values are given by the `Semantic`
            enum. Default is set to `Semantic.DEFAULT`.
        :param kwargs: additional kafka consumer configurations as
            keyword arguments
    """

    def __init__(self, *topics, semantic=Semantic.DEFAULT, **kwargs):
        self._topics = topics
        self._semantic = semantic
        self._config = {}
        # Extract kafka config options from keyword arguments
        for option in BaseConsumer.DEFAULT_CONFIG:
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
        consumer_cls = CONSUMER_FACTORY.get(self._semantic, DefaultKafkaConsumer)
        self._consumer = consumer_cls(*self._topics, **config)

    def start(self):
        self.container.spawn_managed_thread(
            self.run, identifier="{}.run".format(self.__class__.__name__)
        )

    def stop(self):
        self._consumer.close()

    def run(self):
        self._consumer.start(self.handle_message)

    def handle_message(self, message):
        args = (message,)
        kwargs = {}
        self.container.spawn_worker(self, args, kwargs)


consume = KafkaConsumer.decorator
