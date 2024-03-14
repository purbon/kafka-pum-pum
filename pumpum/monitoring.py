import requests
from prometheus_client.parser import text_string_to_metric_families


class ClusterMonitor:

    def __init__(self, broker):
        self.broker = broker
        self.metrics_to_watch = []

    def query(self):
        metrics = requests.get(f"http://{self.broker}")
        content = metrics.text
        response = []
        for family in text_string_to_metric_families(content):
            for sample in family.samples:
                if len(self.metrics_to_watch) == 0 or self.metrics_to_watch.__contains__(sample.name):
                    print("Name: {0} Labels: {1} Value: {2}".format(*sample))
                    response.append({'name': sample.name, 'value': sample.value, 'labels': sample.labels })
        return response

    def register_metrics(self, metrics_to_watch):
        self.metrics_to_watch += metrics_to_watch
