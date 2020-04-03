"""
    File contains CommitsManager to implementing different Kafka message delivery strategies.
"""

from enum import Enum


class DeliveryStrategy(Enum):
    """
        Different supported message delivery strategies.
    """
    AT_MOST_ONCE = "AtMostOnce"
    AT_LEAST_ONCE = "AtLeastOnce"
    EXACTLY_ONCE = "ExactlyOnce"


class CommitsManager:
    """
        Commits commits handling configured message delivery strategy.
    """
