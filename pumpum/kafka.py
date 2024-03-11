import random
import time
from faker import Faker

from confluent_kafka import Producer, Consumer

import socket

from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic, KafkaError


class KafkaClient:

    def __init__(self, bootstrap_servers="localhost:9092"):
        self.conf = {'bootstrap.servers': bootstrap_servers,
                     'client.id': socket.gethostname()}
        self.adminClient = AdminClient(self.conf)

    def produce_for(self, topic, number_of=1000):
        producer = Producer(self.conf)
        try:
            faker = Faker()
            for i in range(number_of):
                value = faker.text()
                producer.produce(topic=topic, key="key", value=value)
        finally:
            producer.flush()

    def consume_from(self, topic):
        self.conf['group.id'] = socket.gethostname() + str(time.time_ns())
        self.conf['auto.offset.reset'] = 'earliest'
        consumer = Consumer(self.conf)

        try:
            consumer.subscribe([topic])
            running = True
            while running:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    running = False
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        running = False
                    elif msg.error():
                        print(msg.error())
                else:
                    print(msg.value())
        finally:
            consumer.close()

    def create_topic(self, name, partitions=3, replicas=1):
        new_topic: NewTopic = NewTopic(topic=name, num_partitions=partitions, replication_factor=replicas)
        response = self.adminClient.create_topics([new_topic])
        return response
