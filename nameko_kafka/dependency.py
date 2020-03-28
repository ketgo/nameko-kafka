"""
    Nameko kafka dependency
"""

from kafka import KafkaProducer as Producer
from nameko.extensions import DependencyProvider


class KafkaProducer(DependencyProvider):
    """
        Kafka dependency for Nameko
    """

    def __init__(self, **kwargs):
        # Gets kafka config options from keyword arguments
        self._config = {
            key: kwargs.pop(key, value) for key, value in Producer.DEFAULT_CONFIG.items()
        }
        self._producer = None
        super(KafkaProducer, self).__init__(**kwargs)

    def start(self):
        self._producer = Producer(**self._config)

    def stop(self):
        self._producer.close()

    def get_dependency(self, worker_ctx):
        return self._producer
