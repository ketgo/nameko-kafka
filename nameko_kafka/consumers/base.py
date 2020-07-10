"""
    Base Kafak Consumer class used to implement different message delivery semantics.
"""

from abc import ABC, abstractmethod

from kafka import KafkaConsumer


class BaseConsumer(ABC, KafkaConsumer):
    """
        Abstract base class for implementing different message delivery semantics
        Kafka consumers.
    """

    @abstractmethod
    def start(self, callback):
        """
            Start consuming message.

            :param callback: callback function to handle consumed messages. The
                function takes ConsumedMessage object as argument.
        """
        raise NotImplementedError()
