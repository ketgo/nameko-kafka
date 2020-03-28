# nameko-kafka

Kafka extension for [Nameko](https://www.nameko.io/) microservice framework. 

## Introduction

This is a Nameko microservice framework [extension](https://nameko.readthedocs.io/en/stable/key_concepts.html) to support Kafka entrypoint and dependency. The motivation behind 
creating this project is this issue [569](https://github.com/nameko/nameko/issues/569). Thus _nameko-kafka_ tries to provide 
a simple implementation extension based on the approach explained by [calumpeterwebb](https://medium.com/@calumpeterwebb/nameko-tutorial-creating-a-kafka-consuming-microservice-c4a7adb804d0).
On topo of that a dependency provider is also included for publishing Kafka messages from within Nameko services.

## Installation

The package is supports Python >= 3.5
```bash
$ pip install nameko-kafka
```

## Usage

The extension can be used for both, service dependency and entrypoint. Example usage for both the cases are shown below:

## Dependency

This is basically a [python-kafka](https://github.com/dpkp/kafka-python) producer in the form of Nameko dependency. 
Nameko uses dependency injection to initiate the producer. You just need to declare it in your service class:

```python
from nameko.rpc import rpc
from nameko_kafka import KafkaProducer


class MyService:
    """
        My microservice
    """
    name = "my-service"
    # Kafak dependency
    producer = KafkaProducer(bootstrap_servers='localhost:1234')
    
    @rpc
    def method(self):
        # Publish message using dependency
        self.producer.send("kafka-topic", value=b"my-message", key=b"my-key")
```

Here `KafkaProducer` accepts all options valid for `python-kafka`'s [KafkaProducer](https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html).

### Entrypoint

You can use the `nameko_kafka.consume` decorator in your services which process Kafka messages:

```python
from nameko_kafka import consume


class MyService:
    """
        My microservice 
    """
    name = "my-service"

    @consume("kafka-topic", group_id="my-group", bootstrap_servers='localhost:1234')
    def method(self, message):
        # Your message handler
        handle_message(message) 
```

The `consume` decorator accepts all the options valid for `python-kafka`'s [KafkaProducer](https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html).

## Configurations

The dependency configurations can be set in nameko [config.yaml]((https://docs.nameko.io/en/stable/cli.html)) file, or 
by environment variables.

### Config File

### Environment Variables

## Developers

## Contributions

Pull requests always welcomed. Thanks!
