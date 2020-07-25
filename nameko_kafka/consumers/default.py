"""
    Basic Kafak consumer class
"""

from kafka import KafkaConsumer


class DefaultConsumer(KafkaConsumer):
    """
        Kafka consumer used by nameko entrypoint as default consumer.
    """

    def start(self, callback):
        """
            Start consuming message.

            :param callback: callback function to handle consumed messages. The
                function takes ConsumedMessage object as argument.
        """
        for message in self:
            callback(message)  # pragma: no cover
