import time

import docker

from pumpum.kafka import KafkaClient

# client = docker.from_env()
# containers = client.containers.list()
# for container in containers:
#    print(container.attrs['Name'])
#
#    out = container.exec_run(cmd="ls /etc")
#    results = out.output.decode('ascii').split("\n")
#    for result in results:
#        print(f"/etc/{result}")

client = docker.from_env()
containers = client.containers.list()
names = [cont.attrs['Name'] for cont in containers]
containers_map = dict(zip(names, containers))


def block_host(hostname):
    container = containers_map[hostname]
    tc_delay_cmd = "tc qdisc change dev eth0 root netem delay 100ms 20ms distribution normal"
    tc_packet_loss_cmd = "tc qdisc change dev eth0 root netem loss 98%"
    response = container.exec_run(cmd=tc_packet_loss_cmd)
    print(response)


if __name__ == "__main__":
    bootstrap_servers = "localhost:9092"
    topic = "my.topic"

    block_host("/broker")

    # kafka_client = KafkaClient(bootstrap_servers=bootstrap_servers)
    # kafka_client.create_topic(name=topic)

    # kafka_client.produce_for(topic=topic, number_of=10)
    # print("Waiting for ...")
    # time.sleep(2)
    # kafka_client.consume_from(topic=topic)
