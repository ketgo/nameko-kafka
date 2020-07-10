"""
    Default Kafka consumer
"""

from .base import BaseConsumer


class DefaultKafkaConsumer(BaseConsumer):
    """
        Default Kafka consumer used by nameko entrypoint.
    """

    def start(self, callback):
        """
            Start consuming messages

            :param callback: message handler callback
        """
        try:
            for message in self:
                callback(message)
        except Exception:
            self.close()
            raise
