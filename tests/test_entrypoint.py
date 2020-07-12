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

        @consume(topic, group_id="test", max_poll_records=2, consumer_timeout_ms=1000)
        def check_message(self, message):
            return message.value

        @consume(topic + "_least_once", group_id="test_least_once", max_poll_records=2, consumer_timeout_ms=1000)
        def check_message_least_once(self, message):
            return message.value + "_least_once".encode()

        @consume(topic + "_most_once", group_id="test_most_once", max_poll_records=2, consumer_timeout_ms=1000)
        def check_message_most_once(self, message):
            return message.value + "_most_once".encode()

    return Service


@pytest.fixture
def container(container_factory, service_cls):
    container = container_factory(service_cls, {})
    container.start()
    yield container
    container.stop()


@pytest.mark.parametrize("suffix", [
    "", "_least_once", "_most_once"
])
def test_entrypoint(container, producer, topic, partition, wait_for_result, entrypoint_tracker, suffix):
    with entrypoint_waiter(container, "check_message{}".format(suffix), callback=wait_for_result, timeout=30):
        producer.send(topic + suffix, b"foo-1", b"test", partition=partition)
        producer.send(topic + suffix, b"foo-2", b"test", partition=partition)

    assert entrypoint_tracker.get_results() == [
        b"foo-1" + suffix.encode(), b"foo-2" + suffix.encode()
    ]


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
