from flask import Flask, request, jsonify
import subprocess
import threading
import logging
import os
import random
import shutil
import parse

# Flask application to manage VR player simulation and experiment execution, exporting the client-side metrics to master
app = Flask(__name__)

# Configuration and network settings
CLIENT_IP = "10.2.64.143"
WORKER_IP = "10.2.64.137"
SERVER_IP = "10.2.64.130"

def create_folders(num_clients, base_path):
    """
    Create directory structure for storing player logs.
    Creates a base path and subdirectories for each client.
    """
    os.makedirs(base_path, exist_ok=True)
    logging.info(f"Base path {base_path} criado ou já existia.")

    # Criar diretórios específicos para cada cliente
    for user in range(1, int(num_clients) + 1):
        user_path = f"{base_path}/user_{user}"
        os.makedirs(user_path, exist_ok=True)
        logging.info(f"Diretório {user_path} criado ou já existia.")

def run_command_for_user(user_argument, base_path, session_duration):
    """
    Execute video player simulation for a specific client.
    Runs the HTTP player with random video and trace file selection.
    """
    video_id = random.randint(1, 2)
    tracefile = random.randint(1, 48)
    # Build command for HTTP player with video and network trace
    cmd = [
        "./http-1.1_p_pl",
        f"{WORKER_IP}:32001", f"v{video_id}", "200",
        f"user_{user_argument}",
        f"../per_video/v{video_id}/u{tracefile}.txt",
        f"{session_duration}", "4", "12", "0", "1", "0", "4",
        f"{base_path}/user_{user_argument}/"
    ]
    try:
        # Execute player command and wait for completion
        subprocess.run(cmd, check=True)
        logging.info(f"Command executed for player_{user_argument}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error to execute the command for player _{user_argument}: {e}")

# POST endpoint to start network monitoring and player simulation
@app.route("/start", methods=["POST"])
def start_players():
    data = request.get_json()
    print(f"Starting players... {data}")

    # Validate input data
    if not data:
        return jsonify({"error": "Missing required fields"}), 400

    # use to create the folders
    num_clients = data["amount_user"]
    num_pods = data["amount_pods"]
    session_duration = data["session_duration"]

    experiment_name = f"{num_clients}_clients_{num_pods}_pods"
    base_path = f"logs/{experiment_name}"

    # Create directories for experiment logs
    create_folders(num_clients, base_path)

    # Create and start a thread for each player client
    threads = []
    for user_argument in range(1, int(num_clients)+1):
        # Creates a thread for each player
        thread = threading.Thread(target=run_command_for_user, args=(user_argument, base_path, session_duration))
        thread.start()
        threads.append(thread)
        logging.info(f"Thread started for player _{user_argument}")

    # Wait for all player threads to complete
    for thread in threads:
        thread.join()

    # Parse experiment results and compute QoE metrics
    try:
        avg_latency = parse.parse_latency(num_clients, base_path)
        qoe_metrics = parse.parse_qoe(num_clients, session_duration, base_path)
    except Exception as e:
        return jsonify({"error": f"Parsing failed: {e}"})

    # Build response payload with experiment metrics
    payload = {
        "num_clients": num_clients,
        "num_pods": num_pods,
        "avg_latency": avg_latency,
        "z1_bit": qoe_metrics['z1_bit'],
        "z2_bit": qoe_metrics['z2_bit'],
        "z3_bit": qoe_metrics['z3_bit'],
        "n_sw_z1": qoe_metrics['n_sw_z1'],
        "n_sw_z2": qoe_metrics['n_sw_z2'],
        "n_sw_z3": qoe_metrics['n_sw_z3'],
        "total_stall": qoe_metrics['total_stall'],
        "start_time": qoe_metrics['start_time'],
        "q_res_z1": qoe_metrics['q_res_z1'],
        "q_res_z2": qoe_metrics['q_res_z2'],
        "q_res_z3": qoe_metrics['q_res_z3'],
        "q_sw_z1": qoe_metrics['q_sw_z1'],
        "q_sw_z2": qoe_metrics['q_sw_z2'],
        "q_sw_z3": qoe_metrics['q_sw_z1'],
        "q_stall": qoe_metrics['stall_term'],
        "QoE": qoe_metrics['overall_qoe']
    }

    # Clean up experiment logs after parsing
    if os.path.exists(base_path):
        shutil.rmtree(base_path)

    return jsonify(payload), 200

if __name__ == "__main__":
    # Start Flask server for experiment control
    app.run(host=f"{CLIENT_IP}", port=8000, debug=True)
