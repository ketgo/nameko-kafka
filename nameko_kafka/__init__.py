"""
    Exposed classes and decorators
"""

import logging

from .dependency import KafkaProducer
from .entrypoint import KafkaConsumer, consume

logging.getLogger(__name__).addHandler(logging.NullHandler())
