import json
from confluent_kafka import Producer


def create_kafka_producer(bootstrap_servers: str) -> Producer:

    """
    Docstring for create_kafka_producer
    :param bootstrap_servers: Description
    :type bootstrap_servers: str
    :return: Description
    :rtype: Any
    """
    return Producer({
    "bootstrap.servers": bootstrap_servers,
    "acks": "all"
    })