"""
    Nameko kafka dependency
"""

import json
import os

from kafka import KafkaProducer as Producer
from nameko.extensions import DependencyProvider

from .constants import KAFKA_PRODUCER_CONFIG_KEY


class KafkaProducer(DependencyProvider):
    """
        Kafka dependency for Nameko
    """

    def __init__(self, **kwargs):
        self._config = {}
        # Extract kafka config options from keyword arguments
        for option in Producer.DEFAULT_CONFIG:
            value = kwargs.pop(option, None)
            if value:
                self._config[option] = value
        self._producer = None
        try:
            super(KafkaProducer, self).__init__(**kwargs)
        except TypeError:
            raise TypeError("Invalid arguments for Kafka producer: '{}' ".format(kwargs))

    def _parse_config(self):
        # Get config from file
        config = self.container.config
        cfg = config.get(KAFKA_PRODUCER_CONFIG_KEY)
        # Check environment variables if config not found in file
        if not cfg:
            _value = os.environ.get(KAFKA_PRODUCER_CONFIG_KEY, "{}")
            cfg = json.loads(_value)
        # Override options from file or env variable with
        # those from keyword arguments.
        for option in self._config:
            cfg[option] = self._config[option]

        return cfg

    def setup(self):
        config = self._parse_config()
        self._producer = Producer(**config)

    def stop(self):
        self._producer.close()

    def get_dependency(self, worker_ctx):
        return self._producer
