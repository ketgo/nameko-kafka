"""
    Tests for kafka entrypoint
"""

import json

import pytest
from nameko.testing.services import entrypoint_waiter

from nameko_kafka import consume, KafkaConsumer
from nameko_kafka.constants import KAFKA_CONSUMER_CONFIG_KEY


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


@pytest.mark.parametrize("file_config,env_config,kwargs,kafka_config", [
    (
            {},
            {
                KAFKA_CONSUMER_CONFIG_KEY: json.dumps({
                    "bootstrap_servers": "mongodb://localhost/default_env",
                    "retry_backoff_ms": 3
                })
            },
            {
                "bootstrap_servers": "mongodb://localhost/default_args",
            },
            {
                "bootstrap_servers": "mongodb://localhost/default_args",
                "retry_backoff_ms": 3
            }
    ),
    (
            {
                KAFKA_CONSUMER_CONFIG_KEY: {
                    "bootstrap_servers": "mongodb://localhost/default_file",
                    "retry_backoff_ms": 5
                }
            },
            {
                KAFKA_CONSUMER_CONFIG_KEY: json.dumps({
                    "bootstrap_servers": "mongodb://localhost/default_env",
                    "retry_backoff_ms": 3
                })
            },
            {},
            {
                "bootstrap_servers": "mongodb://localhost/default_file",
                "retry_backoff_ms": 5
            }
    ),
    (
            {
                KAFKA_CONSUMER_CONFIG_KEY: {
                    "bootstrap_servers": "mongodb://localhost/default_file",
                    "retry_backoff_ms": 5
                }
            },
            {
                KAFKA_CONSUMER_CONFIG_KEY: json.dumps({
                    "bootstrap_servers": "mongodb://localhost/default_env",
                    "retry_backoff_ms": 3
                })
            },
            {
                "retry_backoff_ms": 4
            },
            {
                "bootstrap_servers": "mongodb://localhost/default_file",
                "retry_backoff_ms": 4
            }
    ),

])
def test_config(topic, mocker, mock_container, file_config, env_config, kwargs, kafka_config):
    mocker.patch('os.environ', env_config)
    dep = KafkaConsumer(topic, **kwargs).bind(mock_container, 'engine')
    dep.container.config = file_config
    assert dep._parse_config() == kafka_config


def test_entrypoint_config_error(topic):
    with pytest.raises(TypeError):
        KafkaConsumer(topic, **{"test": 0})
