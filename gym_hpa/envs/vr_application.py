import os
import csv
import datetime
from datetime import datetime, timedelta
import logging
import time
from statistics import mean
import requests
import random
import subprocess
import joblib
import gymnasium as gym
import numpy as np
import pandas as pd
from gymnasium import spaces
from gymnasium.utils import seeding
from .common import BASE_PATH, FLASK_URL, WORKER_IP, USER, INTERFACE

# Number of Requests - Discrete Event
from gym_hpa.envs.deployment import get_max_cpu, get_max_traffic, get_max_latency, get_vr_list, get_max_stall_duration, get_max_stall_count, get_session_duration, get_max_tiles, get_max_sw
from gym_hpa.envs.util import save_to_csv, get_num_pods, get_qoe_reward

# MIN and MAX Replication
MIN_REPLICATION = 1
MAX_REPLICATION = 30

NUM_ACTIONS = MAX_REPLICATION

# MIN and MAX clients
MIN_CLIENTS = 0
MAX_CLIENTS = 30

# MIN and MAX delay in ms
MIN_DELAY = 0
MAX_DELAY = 20

MAX_STEPS = 150  # MAX Number of steps per episode

# Deployments
DEPLOYMENTS = ["vr-deployment"]

# Dynamically generate the moves list for your logging
MOVES = [f"ScaleTo-{i}" for i in range(MIN_REPLICATION, MAX_REPLICATION + 1)]

# IDs
ID_DEPLOYMENTS = 0
ID_MOVES = 1
ID_VR = 0

# Reward objective
QOE = 'qoe'

class VrLearning(gym.Env):
    """Horizontal Scaling for VR in Kubernetes - an OpenAI gym environment"""

    metadata = {'render.modes': ['human', 'ansi', 'array']}

    def __init__(self, experiment_name, k8s=False, goal_reward="qoe", waiting_period=0.3):
        # Define action and observation space
        # They must be gym.spaces objects

        super(VrLearning, self).__init__()

        self.k8s = k8s
        self.name = "vr_application_gym"
        self.experiment_name = experiment_name
        self.__version__ = "0.0.1"
        self.seed()
        self.goal_reward = goal_reward
        self.waiting_period = waiting_period  # seconds to wait after action

        logging.info("[Init] Env: {} | K8s: {} | Version {} |".format(self.name, self.k8s, self.__version__))

        # Current Step
        self.current_step = 0

        # Actions identified by integers 0-n -> 15 actions!
        self.num_actions = NUM_ACTIONS

        # Multi-Discrete
        # Deployment: Discrete 11
        # Action: Discrete 9 - None[0], Add-1[1], Add-2[2], Add-3[3], Add-4[4],
        #                      Stop-1[5], Stop-2[6], Stop-3[7], Stop-4[8]

        self.action_space = spaces.Discrete(self.num_actions)

        self.min_pods = MIN_REPLICATION
        self.max_pods = MAX_REPLICATION

        self.min_clients = MIN_CLIENTS
        self.max_clients = MAX_CLIENTS

        self.min_delay = MIN_DELAY
        self.max_delay = MAX_DELAY

        self.num_apps = 1

        # Deployment Data
        self.deploymentList = get_vr_list(self.k8s, self.min_pods, self.max_pods)

        # Logging Deployment
        for d in self.deploymentList:
            d.print_deployment()

        self.observation_space = self.get_observation_space()

        # Action and Observation Space
        # logging.info("[Init] Action Spaces: " + str(self.action_space))
        # logging.info("[Init] Observation Spaces: " + str(self.observation_space))

        self.current_clients = MIN_CLIENTS
        self.current_delay = MIN_DELAY

        # Info
        self.total_reward = None
        self.avg_pods = []
        self.avg_latency = []

        # episode over
        self.episode_over = False
        self.info = {}

        # Keywords for Reward calculation
        self.constraint_max_pod_replicas = False
        self.constraint_min_pod_replicas = False
        self.cost_weight = 0  # add here a value to consider cost in the reward function

        self.time_start = 0
        self.execution_time = 0
        self.episode_count = 0
        self.file_results = "results.csv"
        self.obs_csv = self.name + "_observation.csv"

        # Create CSV files
        print("Creating file")
        if self.k8s:
            base_path = BASE_PATH + "/real/"
        else:
            base_path = BASE_PATH + "/simulated/"

            print("Loading Digital Twin...")
            self.digital_twin = joblib.load("/home/bernardoacp/Documents/Materiais-de-Estudo/vr-learning/models/xgb_surrogate_model.pkl")
            print("Running in Digital Twin mode.")

        self.csv_file_path = base_path + self.deploymentList[ID_VR].namespace + f"/{self.experiment_name}/" + self.obs_csv
        os.makedirs(os.path.dirname(self.csv_file_path), exist_ok=True)
        if not os.path.isfile(self.csv_file_path):
            self.create_csv_file(self.csv_file_path)

    def run_remote_command(self, command, user=USER, ip=WORKER_IP):
        """Executes a command on the worker node via SSH."""
        ssh_cmd = ["ssh", f"{user}@{ip}", command]
        try:
            subprocess.run(
                ssh_cmd,
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,  
                text=True,
            )
        except subprocess.CalledProcessError as e:
            print(f"Warning: Remote command failed: {command}")
            print(e.stderr)

    def set_network(self, delay_ms, interface=INTERFACE):
        """Applies tc netem rules on the worker node."""
        clean_cmd = f"sudo tc qdisc del dev {interface} root"
        self.run_remote_command(clean_cmd)

        if delay_ms > 0:
            netem_cmd = f"sudo tc qdisc add dev {interface} root netem delay {delay_ms}ms"

            self.run_remote_command(netem_cmd)

    # revision here!
    def step(self, action):
        if self.current_step == 1:
            if not self.k8s:
                self.simulation_update()

            self.time_start = time.time()


        if self.current_step > 1:
            # Client Random Walk
            client_step = np.random.choice([-2, -1, 0, 1, 2])
            self.current_clients = max(0, min(MAX_CLIENTS, self.current_clients + client_step))
            
            # Delay Random Walk
            delay_step = np.random.choice([-2, -1, 0, 1, 2])
            self.current_delay = max(0, min(MAX_DELAY, self.current_delay + delay_step))

        self.take_action(action, 0)

        # Wait a few seconds if on real k8s cluster
        if self.k8s:
            time.sleep(self.waiting_period)  # Wait a few seconds...

            self.deploymentList[ID_VR].network_delay = self.current_delay
            self.deploymentList[ID_VR].num_clients = self.current_clients
            
            # Set random delay
            self.set_network(delay_ms=self.current_delay)

            # Trigger the VR session
            payload = {
                "amount_user": self.current_clients,
                "amount_pods": self.deploymentList[ID_VR].num_pods,
                "session_duration": get_session_duration()
            }
            
            data = {} 
            try:
                response = requests.post(FLASK_URL, json=payload, timeout=300)
                if response.status_code == 200:
                    data = response.json()
                    # Save client-side metrics
            except Exception as e:
                logging.error(f"Failed to start VR session: {e}")

            # Update observation before reward calculation:
            for d in self.deploymentList:
                d.update_obs_k8s()
                d.update_obs_client(data)
        else:
            self.simulation_update()

        # Get reward
        reward = self.get_reward
        self.total_reward += reward

        self.avg_pods.append(get_num_pods(self.deploymentList))
        self.avg_latency.append(self.deploymentList[ID_VR].latency)

        # Print Step and Total Reward
        # if self.current_step == MAX_STEPS:
        logging.info('[Step {}] | Action (Deployment): {} | Action (Move): {} | Reward: {} | Total Reward: {}'.format(
            self.current_step, DEPLOYMENTS[0], MOVES[action], reward, self.total_reward))

        raw_ob, norm_ob = self.get_state()
        self.save_obs_to_csv(self.csv_file_path, np.array(raw_ob), action, reward, self.episode_over)

        self.info = dict(
            total_reward=self.total_reward,
        )

        # Update Reward Keywords
        self.constraint_max_pod_replicas = False
        self.constraint_min_pod_replicas = False

        if self.current_step == MAX_STEPS:
            self.episode_count += 1
            self.execution_time = time.time() - self.time_start

            # logging.info('Avg. latency : {} ', float("{:.3f}".format(mean(self.avg_latency))))
            save_to_csv(self.file_results, self.episode_count, mean(self.avg_pods), mean(self.avg_latency),
                        self.total_reward, self.execution_time)

        terminated = self.episode_over
        truncated = False

        # return ob, reward, self.episode_over, self.info
        return np.array(norm_ob, dtype=np.float32), float(reward), terminated, truncated, self.info

    def seed(self, seed=None):
        self.np_random, seed = seeding.np_random(seed)
        return [seed]

    def reset(self, seed=None, options=None):
        """
        Reset the state of the environment and returns an initial observation.
        Returns
        -------
        observation (object): the initial observation of the space.
        """
        self.current_step = 0
        self.episode_over = False
        self.total_reward = 0
        self.avg_pods = []
        self.avg_latency = []

        self.constraint_max_pod_replicas = False
        self.constraint_min_pod_replicas = False

        # Deployment Data
        self.deploymentList = get_vr_list(self.k8s, self.min_pods, self.max_pods)

        # Random initializer
        self.current_clients = random.randint(self.min_clients, self.max_clients)
        self.current_delay = random.randint(self.min_delay, self.max_delay)

        return np.array(self.get_state()[1], dtype=np.float32), self.info

    def render(self, mode='human', close=False):
        # Render the environment to the screen
        return

    def take_action(self, action, id):
        self.current_step += 1

        # Stop if MAX_STEPS
        if self.current_step == MAX_STEPS:
            # logging.info('[Take Action] MAX STEPS achieved, ending ...')
            self.episode_over = True

        # Map the action (0 to 29) to the target replicas (1 to 30)
        target_replicas = int(action) + MIN_REPLICATION
        
        # Send the declarative command to the deployment
        self.deploymentList[id].scale_to(target_replicas)

    @property
    def get_reward(self):
        """ Calculate Rewards """
        # Reward Calculation
        return self.calculate_reward()

    def get_state(self):
        d = self.deploymentList[ID_VR]
        # Get raw observation
        raw_ob = [
        d.num_clients, d.network_delay, d.num_pods,
        d.cpu_usage, d.max_cpu, d.received_traffic, d.transmit_traffic
        ]
        for metric in d.client_metrics:
            raw_ob.append(getattr(d, metric))

        # Define maximums
        raw_max = [
            self.max_clients, self.max_delay, self.max_pods,
            get_max_cpu(), get_max_cpu(), get_max_traffic(), get_max_traffic(),
            get_max_latency(), get_max_stall_duration(), get_max_stall_count()
        ]

        # Define  minimums 
        raw_min = [self.min_clients, self.min_delay, self.min_pods] + [0.0]*7

        # Normalize to [0, 1]
        norm_ob = []
        for i in range(len(raw_ob)):
            range_val = raw_max[i] - raw_min[i] 
            val = (raw_ob[i] - raw_min[i]) / range_val
            # Clip to ensure it strictly stays in [0, 1] bounds 
            norm_ob.append(max(0.0, min(1.0, val)))

        return tuple(raw_ob), tuple(norm_ob)

    def get_observation_space(self):
        # 22 dimensions bounded between 0.0 and 1.0
        return spaces.Box(
            low=np.float32(0.0), 
            high=np.float32(1.0), 
            shape=(22,), 
            dtype=np.float32
        )

    def calculate_reward(self):
        reward = get_qoe_reward(self.deploymentList)
        return reward

    def simulation_update(self):
        """
        Uses the Model to predict the infrastructure and application metrics based on the current state.
        """
        if self.current_step == 1:
            # On the first step, there is no transition delta.
            self.deploymentList[0].num_previous_pods = self.deploymentList[0].num_pods
        
        # 1. Gather the Input Vector
        current_pods = self.deploymentList[0].num_previous_pods
        target_pods = self.deploymentList[0].num_pods
        num_clients = self.current_clients
        network_delay = self.current_delay
        
        X_input = np.array([[current_pods, target_pods, num_clients, network_delay]])
        
        # 2. Predict the Outcomes
        # Outputs: [stall_duration, stall_count, latency, cpu_usage, max_cpu, traffic_in, traffic_out]
        predictions = self.digital_twin.predict(X_input)[0]
        
        # 3. Map to the Deployment Attributes (Enforce physical reality with max(0, x))
        self.deploymentList[0].stall_duration = max(0.0, float(predictions[0]))
        self.deploymentList[0].stall_count = max(0.0, float(predictions[1]))
        self.deploymentList[0].latency = max(0.0, float(predictions[2]))
        self.deploymentList[0].cpu_usage = max(0.0, float(predictions[3]))
        self.deploymentList[0].max_cpu = max(0.0, float(predictions[4]))
        self.deploymentList[0].received_traffic = max(0.0, float(predictions[5]))
        self.deploymentList[0].transmit_traffic = max(0.0, float(predictions[6]))


    def save_obs_to_csv(self, obs_file, obs, action, reward, done):
        """
        Saves the transition (s, a, r, d) to CSV for offline Decision Transformer training.
        """
        file = open(obs_file, 'a+', newline='')  # append
        date = (datetime.now() + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
        # Define headers
        fields = ['date', 'action', 'reward', 'done']
        # Infrastructure fields
        infra_metrics = ['num_clients', 'network_delay', 'num_pods', 'cpu_usage', 'max_cpu', 'traffic_in', 'traffic_out']

        for d in self.deploymentList:
            for metric in infra_metrics:
                fields.append(f"{d.name}_{metric}")

            # Client fields
            for metric in d.client_metrics:
                fields.append(f"{d.name}_{metric}")

        with file:
            writer = csv.DictWriter(file, fieldnames=fields)
            
            # Build the row dictionary
            row = {
                'date': date,
                'action': action,
                'reward': reward,
                'done': int(done),
                'vr-deployment_num_clients': int(obs[0]),
                'vr-deployment_network_delay': int(obs[1]),
                'vr-deployment_num_pods': int(obs[2]),
                'vr-deployment_cpu_usage': int(obs[3]),
                'vr-deployment_max_cpu': int(obs[4]),
                'vr-deployment_traffic_in': int(obs[5]),
                'vr-deployment_traffic_out': int(obs[6])
            }
            
            # Dynamically add the rest of the metrics from the observation array
            for i, metric in enumerate(self.deploymentList[ID_VR].client_metrics):
                row['vr-deployment_' + metric] = obs[7 + i]
                
            writer.writerow(row)
            

    def create_csv_file(self, file_name):
        file = open(file_name, 'w', newline='')
        fields = ['date', 'action', 'reward', 'done']
        for d in self.deploymentList:
            fields.append(d.name + '_num_clients')
            fields.append(d.name + '_network_delay')
            fields.append(d.name + '_num_pods')
            fields.append(d.name + '_cpu_usage')
            fields.append(d.name + '_max_cpu')
            fields.append(d.name + '_traffic_in')
            fields.append(d.name + '_traffic_out')
            
            for metric in d.client_metrics:
                fields.append(d.name + '_' + metric)

        with file:
            writer = csv.DictWriter(file, fieldnames=fields)
            writer.writeheader()  # write header
