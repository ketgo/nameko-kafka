"""
    Exposed classes and decorators
"""

import logging

from .dependency import KafkaProducer
from .entrypoint import KafkaConsumer, Semantic, consume

logging.getLogger(__name__).addHandler(logging.NullHandler())  # pragma: no cover
