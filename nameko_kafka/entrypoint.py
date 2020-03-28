"""
    Nameko kafka entrypoint
"""

from functools import partial

from kafka import KafkaConsumer as Consumer
from nameko.extensions import Entrypoint


class KafkaConsumer(Entrypoint):
    """
        Kafak consumer extension for Nameko entrypoint.

        :param topics: list of kafka topics to consume
        :param kwargs: additional kafka consumer configurations as
                       keyword arguments
    """

    def __init__(self, *topics, **kwargs):
        self._topics = topics
        # Gets kafka config options from keyword arguments
        self._config = {
            key: kwargs.pop(key, value) for key, value in Consumer.DEFAULT_CONFIG.items()
        }
        self._consumer = None
        super(KafkaConsumer, self).__init__(**kwargs)

    def setup(self):
        self._consumer = Consumer(*self._topics, **self._config)

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
