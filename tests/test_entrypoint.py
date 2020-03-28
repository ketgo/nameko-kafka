"""
    Tests for kafka entrypoint
"""

import pytest
from nameko.testing.services import entrypoint_waiter

from nameko_kafka import consume


@pytest.fixture
def service_cls(topic):
    class Service:
        name = "kafka-test"

        @consume(topic, group_id=topic)
        def check_message(self, message):
            return message.value

    return Service


@pytest.fixture
def container(container_factory, service_cls, topic):
    container = container_factory(service_cls, {})
    container.start()
    yield container
    container.stop()


def test_entrypoint(container, producer, topic, partition):
    with entrypoint_waiter(container, "check_message", timeout=1) as res:
        producer.send(topic, b"foo", b"test", partition=partition)

    assert res.get() == b"foo"


def test_entrypoint_multi_event(container, producer, topic, partition, wait_for_result, entrypoint_tracker):
    with entrypoint_waiter(container, "check_message", callback=wait_for_result, timeout=1):
        producer.send(topic, b"foo-1", b"test", partition=partition)
        producer.send(topic, b"foo-2", b"test", partition=partition)

    assert entrypoint_tracker.get_results() == [b"foo-1", b"foo-2"]
