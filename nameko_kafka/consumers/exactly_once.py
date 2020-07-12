"""
    Exactly once semantic Kafka message consumer
"""

from .default import DefaultConsumer


class ExactlyOnceConsumer(DefaultConsumer):
    """
        Exactly once semantic Kafka message consumer.
    """
