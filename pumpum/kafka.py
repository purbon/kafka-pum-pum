import random
import time
from faker import Faker

from confluent_kafka import Producer, Consumer

import socket

from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic, KafkaError


class KafkaClient:

    def __init__(self, queue=None, bootstrap_servers="localhost:9092"):
        self.conf = {'bootstrap.servers': bootstrap_servers,
                     'client.id': socket.gethostname()}
        self.adminClient = AdminClient(self.conf)
        self.produced_messages = 0
        self.consumed_messages = 0
        self.running = None
        self.queue = queue
        self.stopped = None

    def produce_for(self, topic, number_of=1000):
        self.conf['acks'] = 'all'
        producer = Producer(self.conf)
        self.produced_messages = 0

        try:
            faker = Faker()
            self.running = True
            self.stopped = False
            for i in range(number_of):
                if not self.running:
                    break
                value = faker.text()
                key_id = random.Random().randint(1, 3)
                producer.produce(topic=topic, key=f"key{key_id}", value=value)
                time.sleep(1 / 100)
                self.queue.put("prod")
        finally:
            pending = producer.flush()
            print(f"pending={pending}")
            self.stopped = True

    def consume_from(self, topic):
        self.conf['group.id'] = socket.gethostname() + str(time.time_ns())
        self.conf['auto.offset.reset'] = 'earliest'
        self.conf['enable.partition.eof'] = 'true'
        consumer = Consumer(self.conf)
        self.consumed_messages = 0
        try:
            consumer.subscribe([topic])
            self.running = True
            self.stopped = False
            while self.running:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue

                if msg.error():
                    print(msg.error())
                    # if msg.error().code() == KafkaError._PARTITION_EOF:
                    #    running = False
                #    elif msg.error():
                #        print(msg.error())
                else:
                    self.queue.put("cons")
                    # print(msg.value())
        finally:
            print("consumer.closing")
            consumer.close()
            self.stopped = True
            print("consumer.closed")

    def stop(self):
        self.running = False

    def is_running(self):
        return not self.stopped

    def create_topic(self, name, partitions=3, replicas=1):
        new_topic: NewTopic = NewTopic(topic=name, num_partitions=partitions, replication_factor=replicas)
        response = self.adminClient.create_topics([new_topic])
        return response
