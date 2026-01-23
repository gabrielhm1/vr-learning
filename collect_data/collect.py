import requests
import subprocess
import time
import csv

# Script to build the environment table by running experiments and collecting metrics (runs on Kubernetes master)

# Configuration and API endpoints
CSV_FILE = "/users/marcosbh/data/sample.csv"
FLASK_URL = "http://10.2.64.143:8000/start"
PROMETHEUS_URL = "http://10.100.246.192:9090/"
DEPLOYMENT_NAME = "vr-deployment"

# Experiment parameters
MAX_PODS = 10
MAX_CLIENTS = 15
SESSION_DURATION = [30, 40, 50, 60, 70, 80, 90] # in seconds

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
        "num_pods", "num_clients", "session_duration_s",
        "cpu_m", "mem_Mi", "net_rx_KBps", "net_tx_KBps",
        "avg_latency_s", "z1_bit", "z2_bit", "z3_bit", 
        "n_sw_z1", "n_sw_z2", "n_sw_z3", 
        "total_stall", "start_time", "q_res_z1", "q_res_z2", "q_res_z3", "q_sw_z1", "q_sw_z2", "q_sw_z3", "q_stall", "QoE"
    ]

    with open(CSV_FILE, "w", newline="") as f:
        csv.writer(f).writerow(csv_columns)

    # Run experiments across all parameter combinations
    for num_pods in range(1, MAX_PODS + 1):
        scale_pods(num_pods)
        for num_clients in range(1, MAX_CLIENTS + 1):
            for session_duration in SESSION_DURATION:
                print(f"Running Experiment: Pods = {num_pods}, Clients = {num_clients}, Duration = {session_duration} s")

                data = run_experiment(num_pods, num_clients, session_duration)

                # Prometheus queries for metrics
                query_cpu = f'avg(sum by (pod) (rate(container_cpu_usage_seconds_total{{namespace="default", pod=~"{DEPLOYMENT_NAME}.*", container="{DEPLOYMENT_NAME}"}}[{session_duration}s])))'
                query_mem = f'avg(sum by (pod) (avg_over_time(container_memory_working_set_bytes{{namespace="default",pod=~"{DEPLOYMENT_NAME}.*",container="{DEPLOYMENT_NAME}"}}[{session_duration}s])))'
                query_rx = f'avg(sum(rate(container_network_receive_bytes_total{{namespace="default", pod=~"{DEPLOYMENT_NAME}.*"}}[{session_duration}s])) by (pod))'
                query_tx = f'avg(sum(rate(container_network_transmit_bytes_total{{namespace="default", pod=~"{DEPLOYMENT_NAME}.*"}}[{session_duration}s])) by (pod))'

                # Fetch and convert metrics to appropriate units
                cpu = int(get_value(fetch_prom(query_cpu)) * 1000)  # millicores
                mem = int(get_value(fetch_prom(query_mem)) / 1048576)  # MiB
                rx = int(get_value(fetch_prom(query_rx)) / 1000)  # KB/s
                tx = int(get_value(fetch_prom(query_tx)) / 1000)  # KB/s

                row_data = {
                    "num_pods": num_pods,
                    "num_clients": num_clients,
                    "session_duration_s": session_duration, 
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

    print("Experiment complete, data saved to data/sample.csv")
