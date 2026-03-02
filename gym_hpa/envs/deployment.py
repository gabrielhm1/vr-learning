import logging
import math
import random
import time
import requests
from kubernetes import client, config 

# Constants
MAX_CPU = 10000  # cpu in m
MAX_MEM = 10000  # memory in MiB
MAX_TRAFFIC = 20000  # MAX Number of requests (in Kbit/s)
MAX_LATENCY = 1000 # latency in ms

MAX_STALL_DURATION = 300 # stall in s
MAX_STALL_COUNT = 300

CPU_WEIGHT = 0.7
MEM_WEIGHT = 0.3

VR_SESSION_DURATION = 60 # session duration in s

# port-forward in k8s cluster
PROMETHEUS_URL = 'http://10.2.64.130:32635/'

# Endpoint of your Kube cluster: kube proxy enabled
HOST = "http://localhost:8080"

# TODO: Add the TOKEN from your cluster!
TOKEN = ""

def get_vr_list(k8s, min, max):
    deployment_list = [
        # 1
        DeploymentStatus(k8s, "vr-deployment", "default", "vr-deployment",
                         "marcosmagnocarvalho/vr-application:v1",
                         max, min, 100, 100, 50, 50)
    ]
    return deployment_list


def get_max_cpu():
    return MAX_CPU

def get_max_mem():
    return MAX_MEM

def get_max_traffic():
    return MAX_TRAFFIC

def get_max_latency():
    return MAX_LATENCY

def get_session_duration():
    return VR_SESSION_DURATION

def get_max_stall_duration():
    return MAX_STALL_DURATION

def get_max_stall_count():
    return MAX_STALL_COUNT

def convert_to_milli_cpu(value):
    new_value = int(value[:-1])
    if value[-1] == "n":
        new_value = int(value[:-1])
        new_value = int(new_value / 1000000)

    return new_value


def change_usage(min, max, max_threshold):
    if max > max_threshold:
        max = max_threshold

    if min < 0:
        min = 0

    return random.randint(min, max)


def convert_to_mega_memory(value):
    last_two = value[-2:]
    new_value = 0

    if last_two == "Ki":
        size = len(value)
        # Slice string to remove last 2 characters
        new_value = int(value[:size - 2])
        new_value = int(new_value / 1000)

    return new_value


class DeploymentStatus:  # Deployment Status (Workload)
    def __init__(self, k8s, name, namespace, container_name, container_image, max_pods, min_pods,
                 cpu_request, cpu_limit, mem_request, mem_limit, threshold=0.75):
        self.name = name
        # namespace
        self.namespace = namespace
        # container_name
        self.container_name = container_name
        # container image
        self.container_image = container_image
        # job name
        self.job_name = "kubernetes-cadvisor"

        # CPU & MEM threshold
        self.threshold = threshold
        # CPU weight for replica calculation
        self.cpu_weight = CPU_WEIGHT
        # MEM weight for replica calculation
        self.mem_weight = MEM_WEIGHT

        # Pod Names
        self.pod_names = ["vr-deployment.*"]
        # MAX Number of Pods
        self.max_pods = max_pods
        # MIN Number of Pods
        self.min_pods = min_pods
        # Number of Pods
        self.num_pods = 1  # Initialize as 1
        # Number of Pods in previous step
        self.num_previous_pods = 1  # Initialize as 1
        # Number of desired replicas
        self.desired_replicas = 1

        # CPU request (in m)
        self.cpu_request = cpu_request
        # CPU limit (in m)
        self.cpu_limit = cpu_limit

        # MEM request (in MiB)
        self.mem_request = mem_request
        # MEM limit (in MiB)
        self.mem_limit = mem_limit

        # CPU Target (in m)
        self.cpu_target = int(self.threshold * self.cpu_request)

        # MEM Target (in MiB)
        self.mem_target = int(self.threshold * self.mem_request)

        self.MAX_CPU = MAX_CPU  # cpu in m
        self.MAX_MEM = MAX_MEM  # memory in MiB
        self.MAX_TRAFFIC = MAX_TRAFFIC  # MAX Number of requests

        # Get dataset
        # self.version = 'v1'
        # self.df = pd.read_csv(
        #     "../../datasets/real/" + self.namespace + "/" + self.version +
        #     "/" + self.namespace + '_' + self.name + '.csv')

        # CPU Usage Aggregated (in m)
        self.cpu_usage = random.randint(1, get_max_cpu())  # sample['cpu'].values[0]

        # MEM Usage Aggregated (in MiB)
        self.mem_usage = random.randint(1, get_max_mem())  # sample['mem'].values[0]

        # Current Requests
        self.received_traffic = random.randint(1, get_max_traffic())  # sample['traffic_in'].values[0]
        self.transmit_traffic = random.randint(1, get_max_traffic())  # sample['traffic_out'].values[0]

        # Throughput PING INLINE
        # self.ping = 0

        # K8s enabled?
        self.k8s = k8s

        # csv file
        self.csv = self.namespace + '_' + self.name + '.csv'

        # time between API calls if failure happens
        self.sleep = 0.2

        # Initialize VR client metrics to 0
        self.client_metrics = [
            'latency', 'stall_duration', 'stall_count',
            'n_720_z1', 'n_1080_z1', 'n_4k_z1',
            'n_720_z2', 'n_1080_z2', 'n_4k_z2',
            'n_720_z3', 'n_1080_z3', 'n_4k_z3',
            'z1_bit', 'z2_bit', 'z3_bit',
            'n_sw_z1', 'n_sw_z2', 'n_sw_z3'
        ]

        for metric in self.client_metrics:
            setattr(self, metric, 0)

        # Network delay (Worker node)
        self.network_delay = 0.0

        # Number of clients
        self.num_clients = 0

        if self.k8s:  # Real env: consider a k8s cluster
            logging.info("[Deployment] Consider a real k8s cluster ... ")
            # out of cluster!
            # config.load_kube_config()

            # In cluster config!
            # config.load_incluster_config()

            # token for VWall cluster
            self.token = TOKEN

            # # Create a configuration object
            # self.config = client.Configuration()
            # self.config.verify_ssl = False
            # self.config.api_key = {"authorization": "Bearer " + self.token}

            # # Specify the endpoint of your Kube cluster: kube proxy enabled
            # self.config.host = HOST

            # # Create a ApiClient with our config
            # self.client = client.ApiClient(self.config)

            # v1 api
            config.load_kube_config()
            self.v1 = client.CoreV1Api()
            # apps v1 api
            self.apps_v1 = client.AppsV1Api()

            # metrics api
            # self.metrics_api = client.CustomObjectsApi(self.client)
            # Get deployment object
            self.deployment_object = self.apps_v1.read_namespaced_deployment(name=self.name, namespace=self.namespace)

            # Update number of Pods
            self.num_pods = self.deployment_object.spec.replicas
            self.num_previous_pods = self.deployment_object.spec.replicas

            # update obs
            self.update_obs_k8s()

        # else: # Simulation Environment
        # Update Desired replicas
        # self.update_replicas()

    def update_obs_k8s(self):
        self.pod_names = []
        pods = self.v1.list_namespaced_pod(namespace=self.namespace)
        for p in pods.items:
            if p.metadata.labels['app'] == self.name:
                self.pod_names.append(p.metadata.name)

        self.cpu_usage = 0
        self.mem_usage = 0
        self.received_traffic = 0
        self.transmit_traffic = 0

        # Previous number of Pods
        self.num_previous_pods = self.deployment_object.spec.replicas

        # Get deployment object
        self.deployment_object = self.apps_v1.read_namespaced_deployment(name=self.name, namespace=self.namespace)

        # Update number of Pods
        self.num_pods = self.deployment_object.spec.replicas

        # logging.info("[Update obs] Current Pods: " + str(self.num_pods))

        # Get received / transmit traffic
        for p in self.pod_names:
            query_cpu = f'avg(sum(rate(container_cpu_usage_seconds_total{{job="{self.job_name}", namespace="{self.namespace}", pod=~"{self.pod_names}", container!="", container!="POD"}}[{get_session_duration()}s])) by (pod))'

            query_mem = f'avg(sum(avg_over_time(container_memory_working_set_bytes{{job="{self.job_name}", namespace="{self.namespace}", pod=~"{self.pod_names}", container!="", container!="POD"}}[{get_session_duration()}s])) by (pod))'

            query_received = f'avg(sum(rate(container_network_receive_bytes_total{{job="{self.job_name}", namespace="{self.namespace}", pod=~"{self.pod_names}"}}[{get_session_duration()}s])) by (pod))'

            query_transmit = f'avg(sum(rate(container_network_transmit_bytes_total{{job="{self.job_name}", namespace="{self.namespace}", pod=~"{self.pod_names}"}}[{get_session_duration()}s])) by (pod))'

            # -------------- CPU ----------------
            results_cpu = self.fetch_prom(query_cpu)
            if results_cpu:
                cpu = int(float(results_cpu[0]['value'][1]) * 1000)  # saved as m
                self.cpu_usage += cpu

            # -------------- MEM ----------------
            results_mem = self.fetch_prom(query_mem)
            if results_mem:
                mem = int(float(results_mem[0]['value'][1]) / 1048576)  # saved as Mi
                self.mem_usage += mem

            # -------------- Received Traffic  ----------------
            results_received = self.fetch_prom(query_received)
            if results_received:
                rec = int(float(results_received[0]['value'][1]))
                rec = int(rec / 1000)  # saved as KBit/s
                self.received_traffic += rec

            # -------------- Transmit Traffic  ----------------
            results_transmit = self.fetch_prom(query_transmit)
            if results_transmit:
                trans = int(float(results_transmit[0]['value'][1]))
                trans = int(trans / 1000)  # saved as KBit/s
                self.transmit_traffic += trans

        # Update Desired replicas
        self.update_replicas()

        return

    def update_obs_client(self, data):
        """
        Updates client-side VR metrics from the Flask response data.
        """
        for metric in self.client_metrics:
            setattr(self, metric, data.get(metric, 0))


    def update_replicas(self):
        # min = 1
        if self.desired_replicas == 0:
            self.desired_replicas = 1

        # max = should be equal to the maximum
        if self.desired_replicas > self.max_pods:
            self.desired_replicas = self.max_pods

        return

    def fetch_prom(self, query):
        try:
            response = requests.get(PROMETHEUS_URL + '/api/v1/query',
                                    params={'query': query})

        except requests.exceptions.RequestException as e:
            print(e)
            print("Retrying in {}...".format(self.sleep))
            time.sleep(self.sleep)
            return self.fetch_prom(query)

        if response.json()['status'] != "success":
            print("Error processing the request: " + response.json()['status'])
            print("The Error is: " + response.json()['error'])
            print("Retrying in {}s...".format(self.sleep))
            time.sleep(self.sleep)
            return self.fetch_prom(query)

        result = response.json()['data']['result']
        return result

    def print_deployment(self):
        logging.info("[Deployment] Name: " + str(self.name))
        logging.info("[Deployment] Namespace: " + str(self.namespace))
        logging.info("[Deployment] Number of pods: " + str(self.num_pods))
        logging.info("[Deployment] Desired Replicas: " + str(self.desired_replicas))
        logging.info("[Deployment] Pod Names: " + str(self.pod_names))
        logging.info("[Deployment] MAX Pods: " + str(self.max_pods))
        logging.info("[Deployment] MIN Pods: " + str(self.min_pods))
        logging.info("[Deployment] CPU Usage (in m): " + str(self.cpu_usage))
        logging.info("[Deployment] MEM Usage (in Mi): " + str(self.mem_usage))
        logging.info("[Deployment] Received traffic (in Kbit/s): " + str(self.received_traffic))
        logging.info("[Deployment] Transmit traffic (in Kbit/s): " + str(self.transmit_traffic))
        logging.info("[Deployment] latency (in ms): " + str(self.latency))

    def update_deployment(self, new_replicas):
        # Get deployment object
        self.deployment_object = self.apps_v1.read_namespaced_deployment(name=self.name, namespace=self.namespace)
        # logging.info(self.deployment_object)

        # Update previous number of pods
        self.num_previous_pods = self.deployment_object.spec.replicas

        # Update replicas
        self.deployment_object.spec.replicas = new_replicas

        # try to patch the deployment
        self.patch_deployment(new_replicas)

    def patch_deployment(self, new_replicas):
        try:
            self.apps_v1.patch_namespaced_deployment(
                name=self.name, namespace=self.namespace, body=self.deployment_object
            )
        except Exception as e:
            print(e)
            print("Retrying in {}s...".format(self.sleep))
            time.sleep(self.sleep)
            return self.update_deployment(new_replicas)

    def deploy_pod_replicas(self, n, env):
        # Deploy pods if possible
        replicas = self.num_pods + n

        # logging.info("Deployment name: " + str(self.name))
        # logging.info("Current replicas: " + str(self.num_pods))
        # logging.info("New replicas: " + str(replicas))

        if replicas <= self.max_pods:
            # logging.info("[Take Action] Add {} Replicas".format(str(n)))
            if self.k8s:  # patch deployment on k8s cluster
                self.update_deployment(replicas)
            else:
                self.num_previous_pods = self.num_pods
                self.num_pods = replicas
            return
        else:
            # logging.info("Constraint: MAX Pod Replicas! Desired replicas: " + str(replicas))
            env.constraint_max_pod_replicas = True

    def terminate_pod_replicas(self, n, env):
        # Terminate pods if possible
        replicas = self.num_pods - n

        # logging.info("Deployment name: " + str(self.name))
        # logging.info("Current replicas: " + str(self.num_pods))
        # logging.info("New replicas: " + str(replicas))

        if replicas >= self.min_pods:
            # logging.info("[Take Action] Terminate {} Replicas".format(str(n)))
            if self.k8s:  # patch deployment on k8s cluster
                self.update_deployment(replicas)
            else:
                self.num_previous_pods = self.num_pods
                self.num_pods = replicas
            return
        else:
            # logging.info("Constraint: MIN Pod Replicas! Desired replicas: " + str(replicas))
            env.constraint_min_pod_replicas = True