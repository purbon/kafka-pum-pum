import json
import queue
import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Process

import docker

import asyncio
import concurrent.futures

from pumpum.deployment import ContainersManager
from pumpum.kafka import KafkaClient


class Reporter:

    def __init__(self, queue):
        self.queue = queue
        self.running = True
        self.produced_messages = 0
        self.consumed_messages = 0

    def collect(self):
        while self.running:
            try:
                elem = self.queue.get(block=True)
                if elem == "prod":
                    self.produced_messages += 1
                else:
                    self.consumed_messages += 1
            except:
                pass

    def stop(self):
        print("Stopping reporter")
        self.running = False

    def get_report(self):
        return {'prod': self.produced_messages, 'cons': self.consumed_messages}


class ClientSwarm:

    def __init__(self, servers, queue):
        self.servers = servers
        self.queue = queue
        self.producer_kafka_client: KafkaClient = KafkaClient(bootstrap_servers=self.servers, queue=self.queue)
        self.consumer_kafka_client: KafkaClient = KafkaClient(bootstrap_servers=self.servers, queue=self.queue)

    def produce_to(self, topic, to_produce_messages):
        self.producer_kafka_client.produce_for(topic=topic, number_of=to_produce_messages)

    def consumer_from(self, topic):
        self.consumer_kafka_client.consume_from(topic=topic)

    def stop(self):
        print("Stopping clients")
        self.__stop_kafka_client(self.producer_kafka_client)
        self.__stop_kafka_client(self.consumer_kafka_client)

    def __stop_kafka_client(self, kafka_client=None):
        kafka_client.stop()
        while kafka_client.is_running():
            time.sleep(1 / 10)


if __name__ == "__main__":
    executor = ThreadPoolExecutor(max_workers=3)

    to_produce_messages = 1000

    bootstrap_servers = "localhost:9092,localhost:9093,localhost:9094"
    myqueue = queue.SimpleQueue()

    reporter = Reporter(queue=myqueue)
    cs = ClientSwarm(servers=bootstrap_servers, queue=myqueue)

    topic = "my.topic"

    client = docker.from_env()
    containers = ContainersManager(client=client)

    kafka_client = KafkaClient(bootstrap_servers=bootstrap_servers)
    kafka_client.create_topic(name=topic, partitions=3, replicas=3)

    time.sleep(1)

    reporter_func = executor.submit(reporter.collect)
    produce_func = executor.submit(cs.produce_to, topic, to_produce_messages)
    consumer_func = executor.submit(cs.consumer_from, topic)

    print("Waiting before killing a host....")
    time.sleep(5)

    node = "/broker1"
    containers.kill_a_host(hostname=node)
    time.sleep(5)
    print("Still consuming and producing....")

    while True:
        consumed = reporter.get_report()["cons"]
        produced = reporter.get_report()["prod"]
        print(f"{consumed}-{produced}")
        if consumed == to_produce_messages:
            break
        time.sleep(1)

    cs.stop()
    reporter.stop()
    time.sleep(1)
    print(reporter.get_report())
    executor.shutdown(wait=False, cancel_futures=True)
