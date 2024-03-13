from signal import SIGKILL

import docker


class ContainersManager:

    def __init__(self, client):
        containers = client.containers.list()
        names = [cont.attrs['Name'] for cont in containers]
        self.containers_map = dict(zip(names, containers))
        self.container_ips = dict(zip(names, self.__container_to_ips(containers=containers)))

    def add_network_delay(self, hostname):
        tc_delay_cmd = "tc qdisc change dev eth0 root netem delay 100ms 20ms distribution normal"

    def add_packet_loss(self, hostname):
        container = self.containers_map[hostname]
        tc_packet_loss_cmd = "tc qdisc change dev eth0 root netem loss 90%"
        response = container.exec_run(cmd=tc_packet_loss_cmd)
        print(response)

    def kill_a_host(self, hostname):
        container = self.containers_map[hostname]
        container.kill(SIGKILL)

    def block_host(self, hostname):
        pass
        # iptables -I INPUT -s 172.24.0.3 -j DROP
        # iptables -I OUTPUT -d 172.24.0.3 -j DROP

    def __container_to_ips(self, containers):
        host_ips = []
        for container in containers:
            networks = container.attrs['NetworkSettings']['Networks']
            for key, value in networks.items():
                host_ips.append(value['IPAddress'])
        return host_ips