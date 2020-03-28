"""
    Tests for kafka dependency
"""

import pytest
from nameko.testing.services import dummy, entrypoint_hook

from nameko_kafka import KafkaProducer


@pytest.fixture
def service_cls(topic, partition):
    class Service:
        name = "kafka-test"
        producer = KafkaProducer()

        @dummy
        def dispatch(self, message, key):
            self.producer.send(topic, value=message, key=key, partition=partition)

    return Service


@pytest.fixture
def container(container_factory, service_cls, topic):
    container = container_factory(service_cls, {})
    container.start()
    yield container
    container.stop()


def test_dependency(container, consumer):
    with entrypoint_hook(container, "dispatch") as hook:
        hook(b"foo", b"test")
        fetch = consumer.poll(timeout_ms=1000, max_records=1)
        for topic in fetch:
            for msg in fetch[topic]:
                assert msg.value == b"foo"
                assert msg.key == b"test"
