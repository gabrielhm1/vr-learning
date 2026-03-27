import requests
import subprocess
import time
import csv
from common import FLASK_URL, PROMETHEUS_URL, WORKER_IP, USER, INTERFACE, CSV_FILE

# Script to build the environment table by running experiments and collecting metrics (runs on Kubernetes master)

# Configuration 
DEPLOYMENT_NAME = "vr-deployment"
NAMESPACE = "default"
JOB_NAME = "kubernetes-cadvisor"

# Experiment parameters
MAX_PODS = 20
MAX_CLIENTS = 30
MAX_DELAY = 20
SESSION_DURATION = 60 # in seconds

def run_remote_command(command):
    """Executes a command on the worker node via SSH."""
    ssh_cmd = ["ssh", f"{USER}@{WORKER_IP}", command]
    try:
        subprocess.run(
            ssh_cmd,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,  # keep stderr while debugging
            text=True,
        )
    except subprocess.CalledProcessError as e:
        print(f"Warning: Remote command failed: {command}")
        print(e.stderr)

def set_network(delay_ms):
    """Applies tc netem rules on the worker node."""
    clean_cmd = f"sudo tc qdisc del dev {INTERFACE} root"
    run_remote_command(clean_cmd)

    if delay_ms > 0:
        netem_cmd = f"sudo tc qdisc add dev {INTERFACE} root netem delay {delay_ms}ms"
        run_remote_command(netem_cmd)

def scale_pods(num_pods):
    """Scale Kubernetes deployment to specified number of replicas and wait for readiness."""
    subprocess.run(["kubectl", "scale", f"deployment/{DEPLOYMENT_NAME}", f"--replicas={num_pods}"], check=True)
    subprocess.run([
        "kubectl", "wait", "--for=condition=Ready", "pod",
        "-l", f"app={DEPLOYMENT_NAME}", "--timeout=60s"
    ], check=False)
    time.sleep(5)

def fetch_prom(query):
    """Fetch metrics from Prometheus with automatic retry on failure."""
    try:
        response = requests.get(PROMETHEUS_URL + '/api/v1/query', params={'query': query})

    except requests.exceptions.RequestException as e:
        print(e)
        print("Retrying in {}...".format(0.2))
        time.sleep(0.2)
        return fetch_prom(query)

    if response.json()['status'] != "success":
        print("Error processing the request: " + response.json()['status'])
        print("The Error is: " + response.json()['error'])
        print("Retrying in {}s...".format(0.2))
        time.sleep(0.2)
        return fetch_prom(query)

    result = response.json()['data']['result']
    return result

def get_value(results):
    """Extract numeric value from Prometheus query result."""
    try:
        if results:
            return float(results[0]['value'][1])
    except (IndexError, ValueError):
        pass
    return 0.0

def run_experiment(num_pods, num_clients, session_duration):
    """Execute experiment via Flask API and return results."""
    payload = {
        "amount_user": num_clients,
        "amount_pods": num_pods,
        "session_duration": session_duration
    }
    try:
        response = requests.post(FLASK_URL, json=payload, timeout=300)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        print(f"Error during request: {e}")
    return {}

if __name__ == "__main__":
    # Initialize CSV file with headers
    csv_columns = [
        "num_pods", "num_clients", "net_delay",
        "cpu_m", "mem_Mi", "net_rx_KBps", "net_tx_KBps",
        "latency", "n_720_z1", "n_1080_z1", "n_4k_z1",
        "n_720_z2", "n_1080_z2", "n_4k_z2",
        "n_720_z3", "n_1080_z3", "n_4k_z3",
        "z1_bit", "z2_bit", "z3_bit",
        "n_sw_z1", "n_sw_z2", "n_sw_z3",
        "total_stall", "stall_count", 
        "start_time", "session_qoe"
    ]

    with open(CSV_FILE, "w", newline="") as f:
        csv.writer(f).writerow(csv_columns)

    # Run experiments across all parameter combinations
    for num_clients in [1, MAX_CLIENTS]:
        for num_pods in range(1, MAX_PODS + 1):
            scale_pods(num_pods)
            for net_delay in range(0, MAX_DELAY + 1):
                set_network(net_delay)
                print(f"Running Experiment: Pods = {num_pods}, Clients = {num_clients}, Delay = {net_delay} ms")

                data = run_experiment(num_pods, num_clients, SESSION_DURATION)

                # Prometheus queries for metrics
                query_cpu = f'avg(sum(rate(container_cpu_usage_seconds_total{{job="{JOB_NAME}", namespace="{NAMESPACE}", pod=~"{DEPLOYMENT_NAME}.*", container!="", container!="POD"}}[{SESSION_DURATION}s])) by (pod))'

                query_mem = f'avg(sum(avg_over_time(container_memory_working_set_bytes{{job="{JOB_NAME}", namespace="{NAMESPACE}", pod=~"{DEPLOYMENT_NAME}.*", container!="", container!="POD"}}[{SESSION_DURATION}s])) by (pod))'

                query_received = f'avg(sum(rate(container_network_receive_bytes_total{{job="{JOB_NAME}", namespace="{NAMESPACE}", pod=~"{DEPLOYMENT_NAME}.*"}}[{SESSION_DURATION}s])) by (pod))'

                query_transmit = f'avg(sum(rate(container_network_transmit_bytes_total{{job="{JOB_NAME}", namespace="{NAMESPACE}", pod=~"{DEPLOYMENT_NAME}.*"}}[{SESSION_DURATION}s])) by (pod))'


                # Fetch and convert metrics to appropriate units
                cpu = int(get_value(fetch_prom(query_cpu)) * 1000)  # millicores
                mem = int(get_value(fetch_prom(query_mem)) / 1048576)  # MiB
                rx = int(get_value(fetch_prom(query_received)) / 1000)  # KB/s
                tx = int(get_value(fetch_prom(query_transmit)) / 1000)  # KB/s

                row_data = {
                    "num_pods": num_pods,
                    "num_clients": num_clients,
                    "net_delay": net_delay,
                    "cpu_m": cpu,
                    "mem_Mi": mem,
                    "net_rx_KBps": rx,
                    "net_tx_KBps": tx,
                    **data  # Unpacks the payload
                }

                # Write experiment results to CSV
                with open(CSV_FILE, "a", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow([row_data.get(col, 0) for col in csv_columns])

    print("Experiment complete, data saved to data/result.csv")
