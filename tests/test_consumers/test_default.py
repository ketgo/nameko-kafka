"""
    Tests for default Kafka consumer
"""

import time

import pytest

from nameko_kafka.consumers.default import DefaultConsumer


@pytest.fixture
def kafka_consumer(topic):
    _consumer = DefaultConsumer(
        topic, group_id=topic, max_poll_records=2, consumer_timeout_ms=1000
    )
    yield _consumer
    _consumer.close()


def test_consumer(topic, partition, producer, kafka_consumer):
    producer.send(topic, b"foo-1", b"test", partition=partition)
    producer.send(topic, b"foo-2", b"test", partition=partition)
    time.sleep(1)

    messages = []

    def handler(msg):
        messages.append(msg.value)

    kafka_consumer.start(handler)

    assert messages == [b"foo-1", b"foo-2"]
