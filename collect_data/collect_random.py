import requests
import subprocess
import time
import csv
import threading
from scipy.stats import qmc
import numpy as np 
from common import SAMPLE_FILE, FLASK_URL, PROMETHEUS_URL, USER, WORKER_IP, INTERFACE

# Configuration
DEPLOYMENT_NAME = "vr-deployment"
SESSION_DURATION = 60 # seconds

DEPLOYMENT_NAME = "vr-deployment"
NAMESPACE = "default"
JOB_NAME = "kubernetes-cadvisor"

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

def scale_pods_async(num_pods):
    """Scales pods without blocking, allowing the VR session to start immediately."""
    subprocess.run(["kubectl", "scale", f"deployment/{DEPLOYMENT_NAME}", f"--replicas={num_pods}"], check=True)

def reset_environment(current_pods):
    """Forces the cluster to the starting state and waits for it to stabilize."""
    print(f"Resetting environment to {current_pods} pods...")
    subprocess.run(["kubectl", "scale", f"deployment/{DEPLOYMENT_NAME}", f"--replicas={current_pods}"], check=True)
    subprocess.run([
        "kubectl", "wait", "--for=condition=Ready", "pod",
        "-l", f"app={DEPLOYMENT_NAME}", "--timeout=30s"
    ], check=False)
    time.sleep(15) # Let CPU settle

def generate_lhs_samples(num_samples=3000):
    """Generates a 4D Latin Hypercube Sample matrix."""
    print(f"Generating {num_samples} LHS samples...")
    # 4 Dimensions: [current_pods, target_pods, clients, delay]
    sampler = qmc.LatinHypercube(d=4)
    sample = sampler.random(n=num_samples)

    # Define bounds: [lower, upper]
    l_bounds = [1, 1, 0, 0]
    u_bounds = [30, 30, 30, 20]
    
    # Scale to our physical K8s boundaries and round to integers
    scaled_samples = qmc.scale(sample, l_bounds, u_bounds)
    return np.round(scaled_samples).astype(int)

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

def main():
    # Generate the matrix
    samples = generate_lhs_samples(3000)
    
    # 2. Setup CSV Headers (4 Inputs, 7 Outputs)
    headers = [
        "current_pods", "target_pods", "num_clients", "network_delay", # Inputs (X)
        "stall_duration", "stall_count", "latency", "cpu_usage", "max_cpu", "traffic_in", "traffic_out" # Outputs (Y)
    ]
    
    with open(SAMPLE_FILE, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(headers)
    
    # 3. Execution Loop
    for i, row in enumerate(samples):
        current_pods, target_pods, num_clients, network_delay = row
        print(f"\n--- Running Sample {i+1}/3000 ---")
        print(f"State: {current_pods} pods | Action: Scale to {target_pods} pods | Clients: {num_clients} | Delay: {network_delay}ms")
        
        # Reset to starting state
        reset_environment(current_pods)
        
        # Apply network delay
        set_network(network_delay)
                
        # If clients = 0, we still scale, but we don't need to ping Flask.
        client_metrics = {"stall_duration": 0, "stall_count": 0, "latency": 0}
        
        if num_clients > 0:
            # Trigger K8s scaling in a background thread 
            scale_thread = threading.Thread(target=scale_pods_async, args=(target_pods,))
            scale_thread.start()
            
            # Immediately trigger Flask players
            payload = {
                "amount_user": int(num_clients),
                "amount_pods": int(target_pods),
                "session_duration": SESSION_DURATION
            }
            try:
                response = requests.post(FLASK_URL, json=payload, timeout=300)
                if response.status_code == 200:
                    data = response.json()
                    client_metrics["stall_duration"] = data.get("stall_duration", 0)
                    client_metrics["stall_count"] = data.get("stall_count", 0)
                    client_metrics["latency"] = data.get("latency", 0)
            except Exception as e:
                print(f"Flask execution failed: {e}")
            
            scale_thread.join()
        else:
            # If 0 clients, just scale and sleep for 60 seconds
            scale_pods_async(target_pods)
            time.sleep(SESSION_DURATION)

        # Step D: Fetch Infrastructure Metrics
        prom_regex = f"{DEPLOYMENT_NAME}.*"

        # Average CPU
        query_cpu = f'avg(sum(rate(container_cpu_usage_seconds_total{{job="{JOB_NAME}", namespace="{NAMESPACE}", pod=~"{prom_regex}", container!="", container!="POD"}}[{SESSION_DURATION}s])) by (pod))'

        # Max CPU
        query_max_cpu = f'max(sum(rate(container_cpu_usage_seconds_total{{job="{JOB_NAME}", namespace="{NAMESPACE}", pod=~"{prom_regex}", container!="", container!="POD"}}[{SESSION_DURATION}s])) by (pod))'

        # Total Received Traffic (Sum of all pods)
        query_received = f'sum(rate(container_network_receive_bytes_total{{job="{JOB_NAME}", namespace="{NAMESPACE}", pod=~"{prom_regex}"}}[{SESSION_DURATION}s]))'

        # Total Transmit Traffic (Sum of all pods)
        query_transmit = f'sum(rate(container_network_transmit_bytes_total{{job="{JOB_NAME}", namespace="{NAMESPACE}", pod=~"{prom_regex}"}}[{SESSION_DURATION}s]))'


        # Fetch and convert metrics to appropriate units
        cpu_usage = int(get_value(fetch_prom(query_cpu)) * 1000)  # millicores
        max_cpu = int(get_value(fetch_prom(query_max_cpu)) * 1000)  # MiB
        received_traffic = int(get_value(fetch_prom(query_received)) / 1000)  # KB/s
        transmit_traffic = int(get_value(fetch_prom(query_transmit)) / 1000)  # KB/s
        
        # Step E: Save to CSV
        with open(SAMPLE_FILE, mode='a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([
                current_pods, target_pods, num_clients, network_delay,
                round(float(client_metrics["stall_duration"]), 3), 
                round(float(client_metrics["stall_count"]), 3), 
                round(float(client_metrics["latency"]), 4),
                cpu_usage, max_cpu, received_traffic,  transmit_traffic
            ])

if __name__ == "__main__":
    main()