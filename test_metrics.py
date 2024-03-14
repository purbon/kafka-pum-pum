from pumpum.monitoring import ClusterMonitor

if __name__ == "__main__":

    monitor = ClusterMonitor(broker="localhost:7101")
    metrics_to_watch = [
            "kafka_server_raft_metrics_current_leader",
             "kafka_server_replicamanager_underreplicatedpartitions",
            "kafka_network_processor_idlepercent",
            "kafka_network_socketserver_networkprocessoravgidlepercent"
    ]
    monitor.register_metrics(metrics_to_watch=metrics_to_watch)
    monitor.query()