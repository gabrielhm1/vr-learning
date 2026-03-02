import os
import csv
import datetime
from datetime import datetime
import logging
import time
from statistics import mean
import requests
import random
import subprocess

import gym
import numpy as np
import pandas as pd
from gym import spaces
from gym.utils import seeding

# Number of Requests - Discrete Event
from gym_hpa.envs.deployment import get_max_cpu, get_max_mem, get_max_traffic, get_max_latency, get_vr_list, get_max_stall_duration, get_max_stall_count, get_session_duration
from gym_hpa.envs.util import save_to_csv, get_num_pods, get_cost_reward, \
    get_latency_reward_online_boutique

# MIN and MAX Replication
MIN_REPLICATION = 1
MAX_REPLICATION = 10

# MIN and MAX clients
MIN_CLIENTS = 0
MAX_CLIENTS = 30

# MIN and MAX delay in ms
MIN_DELAY = 0
MAX_DELAY = 20

MAX_STEPS = 25  # MAX Number of steps per episode

FLASK_URL = "http://10.2.64.143:8000/start"

WORKER_IP = "10.2.64.137"
USER = "marcosbh"
INTERFACE = "eno1"

# Possible Actions (Discrete)
ACTION_DO_NOTHING = 0
ACTION_ADD_1_REPLICA = 1
ACTION_ADD_2_REPLICA = 2
ACTION_ADD_3_REPLICA = 3
ACTION_ADD_4_REPLICA = 4
ACTION_ADD_5_REPLICA = 5
ACTION_ADD_6_REPLICA = 6
ACTION_ADD_7_REPLICA = 7
ACTION_TERMINATE_1_REPLICA = 8
ACTION_TERMINATE_2_REPLICA = 9
ACTION_TERMINATE_3_REPLICA = 10
ACTION_TERMINATE_4_REPLICA = 11
ACTION_TERMINATE_5_REPLICA = 12
ACTION_TERMINATE_6_REPLICA = 13
ACTION_TERMINATE_7_REPLICA = 14

# Deployments
DEPLOYMENTS = ["vr-deployment"]

# Action Moves
MOVES = ["None", "Add-1", "Add-2", "Add-3", "Add-4", "Add-5", "Add-6", "Add-7",
         "Stop-1", "Stop-2", "Stop-3", "Stop-4", "Stop-5", "Stop-6", "Stop-7"]

# IDs
ID_DEPLOYMENTS = 0
ID_MOVES = 1

ID_VR = 0

# Reward objectives
LATENCY = 'latency'
COST = 'cost'
# TODO: stall objective

class VrLearning(gym.Env):
    """Horizontal Scaling for VR in Kubernetes - an OpenAI gym environment"""

    metadata = {'render.modes': ['human', 'ansi', 'array']}

    def __init__(self, k8s=False, goal_reward="cost", waiting_period=0.3):
        # Define action and observation space
        # They must be gym.spaces objects

        super(VrLearning, self).__init__()

        self.k8s = k8s
        self.name = "vr_application_gym"
        self.__version__ = "0.0.1"
        self.seed()
        self.goal_reward = goal_reward
        self.waiting_period = waiting_period  # seconds to wait after action

        logging.info("[Init] Env: {} | K8s: {} | Version {} |".format(self.name, self.k8s, self.__version__))

        # Current Step
        self.current_step = 0

        # Actions identified by integers 0-n -> 15 actions!
        self.num_actions = 15

        # Multi-Discrete
        # Deployment: Discrete 11
        # Action: Discrete 9 - None[0], Add-1[1], Add-2[2], Add-3[3], Add-4[4],
        #                      Stop-1[5], Stop-2[6], Stop-3[7], Stop-4[8]

        self.action_space = spaces.Discrete(self.num_actions)

        # Observations: 22 Metrics! -> 2 * 11 = 22
        # "number_pods"                     -> Number of deployed Pods
        # "cpu_usage_aggregated"            -> via metrics-server
        # "mem_usage_aggregated"            -> via metrics-server
        # "cpu_requests"                    -> via metrics-server/pod
        # "mem_requests"                    -> via metrics-server/pod
        # "cpu_limits"                      -> via metrics-server
        # "mem_limits"                      -> via metrics-server
        # "lstm_cpu_prediction_1_step"      -> via pod annotation
        # "lstm_cpu_prediction_5_step"      -> via pod annotation
        # "average_number of requests"      -> Prometheus metric: sum(rate(http_server_requests_seconds_count[5m]))

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
        self.csv_file_path = "../../datasets/real/" + self.deploymentList[0].namespace + "/v1/" + self.obs_csv
        os.makedirs(os.path.dirname(self.csv_file_path), exist_ok=True)
        if not os.path.isfile(self.csv_file_path):
            self.create_csv_file = self.create_csv_file(self.csv_file_path)

        self.df = pd.read_csv(self.csv_file_path)

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

        # Execute one time step within the environment
        self.take_action(action, 0)

        # Wait a few seconds if on real k8s cluster
        if self.k8s:
            if action != ACTION_DO_NOTHING \
                    and self.constraint_min_pod_replicas is False \
                    and self.constraint_max_pod_replicas is False:
                # logging.info('[Step {}] | Waiting {} seconds for enabling action ...'
                # .format(self.current_step, self.waiting_period))
                time.sleep(self.waiting_period)  # Wait a few seconds...
            
            network_delay = random.randint(self.min_delay, self.max_delay)
            num_clients = random.randint(self.min_clients, self.max_clients)

            self.deploymentList[0].network_delay = network_delay
            self.deploymentList[0].num_clients = num_clients
            
            # Set random delay
            self.set_network(delay_ms=network_delay)

            # Trigger the VR session
            payload = {
                "amount_user": num_clients,
                "amount_pods": self.deploymentList[0].num_pods,
                "session_duration": get_session_duration()
            }
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
        self.avg_latency.append(self.deploymentList[0].latency)

        # Print Step and Total Reward
        # if self.current_step == MAX_STEPS:
        logging.info('[Step {}] | Action (Deployment): {} | Action (Move): {} | Reward: {} | Total Reward: {}'.format(
            self.current_step, DEPLOYMENTS[0], MOVES[action], reward, self.total_reward))

        ob = self.get_state()
        date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.save_obs_to_csv(self.csv_file_path, np.array(ob), date, self.deploymentList[0].latency)

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

        # return ob, reward, self.episode_over, self.info
        return np.array(ob), reward, self.episode_over, self.info

    def seed(self, seed=None):
        self.np_random, seed = seeding.np_random(seed)
        return [seed]

    def reset(self):
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

        return np.array(self.get_state())

    def render(self, mode='human', close=False):
        # Render the environment to the screen
        return

    def take_action(self, action, id):
        self.current_step += 1

        # Stop if MAX_STEPS
        if self.current_step == MAX_STEPS:
            # logging.info('[Take Action] MAX STEPS achieved, ending ...')
            self.episode_over = True

        # ACTIONS
        if action == ACTION_DO_NOTHING:
            # logging.info("[Take Action] SELECTED ACTION: DO NOTHING ...")
            pass

        elif action == ACTION_ADD_1_REPLICA:
            # logging.info("[Take Action] SELECTED ACTION: ADD 1 Replica ...")
            self.deploymentList[id].deploy_pod_replicas(1, self)

        elif action == ACTION_ADD_2_REPLICA:
            # logging.info("[Take Action] SELECTED ACTION: ADD 2 Replicas ...")
            self.deploymentList[id].deploy_pod_replicas(2, self)

        elif action == ACTION_ADD_3_REPLICA:
            # logging.info("[Take Action] SELECTED ACTION: ADD 3 Replicas ...")
            self.deploymentList[id].deploy_pod_replicas(3, self)

        elif action == ACTION_ADD_4_REPLICA:
            # logging.info("[Take Action] SELECTED ACTION: ADD 4 Replicas ...")
            self.deploymentList[id].deploy_pod_replicas(4, self)

        elif action == ACTION_ADD_5_REPLICA:
            # logging.info("[Take Action] SELECTED ACTION: ADD 5 Replicas ...")
            self.deploymentList[id].deploy_pod_replicas(5, self)

        elif action == ACTION_ADD_6_REPLICA:
            # logging.info("[Take Action] SELECTED ACTION: ADD 6 Replicas ...")
            self.deploymentList[id].deploy_pod_replicas(6, self)

        elif action == ACTION_ADD_7_REPLICA:
            # logging.info("[Take Action] SELECTED ACTION: ADD 7 Replicas ...")
            self.deploymentList[id].deploy_pod_replicas(7, self)

        elif action == ACTION_TERMINATE_1_REPLICA:
            # logging.info("[Take Action] SELECTED ACTION: TERMINATE 1 Replica ...")
            self.deploymentList[id].terminate_pod_replicas(1, self)

        elif action == ACTION_TERMINATE_2_REPLICA:
            # logging.info("[Take Action] SELECTED ACTION: TERMINATE 2 Replicas ...")
            self.deploymentList[id].terminate_pod_replicas(2, self)

        elif action == ACTION_TERMINATE_3_REPLICA:
            # logging.info("[Take Action] SELECTED ACTION: TERMINATE 3 Replicas ...")
            self.deploymentList[id].terminate_pod_replicas(3, self)

        elif action == ACTION_TERMINATE_4_REPLICA:
            # logging.info("[Take Action] SELECTED ACTION: TERMINATE 4 Replicas ...")
            self.deploymentList[id].terminate_pod_replicas(4, self)

        elif action == ACTION_TERMINATE_5_REPLICA:
            # logging.info("[Take Action] SELECTED ACTION: TERMINATE 5 Replicas ...")
            self.deploymentList[id].terminate_pod_replicas(5, self)

        elif action == ACTION_TERMINATE_6_REPLICA:
            # logging.info("[Take Action] SELECTED ACTION: TERMINATE 6 Replicas ...")
            self.deploymentList[id].terminate_pod_replicas(6, self)

        elif action == ACTION_TERMINATE_7_REPLICA:
            # logging.info("[Take Action] SELECTED ACTION: TERMINATE 7 Replicas ...")
            self.deploymentList[id].terminate_pod_replicas(7, self)

        else:
            logging.info('[Take Action] Unrecognized Action: ' + str(action))

    @property
    def get_reward(self):
        """ Calculate Rewards """
        # Reward based on Keyword!
        if self.constraint_max_pod_replicas:
            if self.goal_reward == COST:
                return -1  # penalty
            elif self.goal_reward == LATENCY:
                return -3000  # penalty

        if self.constraint_min_pod_replicas:
            if self.goal_reward == COST:
                return -1  # penalty
            elif self.goal_reward == LATENCY:
                return -3000  # penalty

        # Reward Calculation
        reward = self.calculate_reward()
        return reward

    def get_state(self):
        # Observations: metrics - 3 Metrics!!
        # "number_pods"
        # "cpu"
        # "mem"
        # "requests"
        d = self.deploymentList[ID_VR]
        # Return ob
        ob = [
                d.num_clients,
                d.network_delay,
                d.num_pods,
                d.desired_replicas,
                d.cpu_usage,
                d.mem_usage,
                d.received_traffic,
                d.transmit_traffic
            ]
        
        for metric in d.client_metrics:
            ob.append(getattr(d, metric))

        return tuple(ob)

    def get_observation_space(self):
            return spaces.Box(
                low=np.array([
                    self.min_clients, # Number of clients
                    self.min_delay, # Network delay
                    self.min_pods,  # Number of Pods  -- 1) recommendationservice
                    self.min_pods,  # Desired Replicas
                    0,  # CPU Usage (in m)
                    0,  # MEM Usage (in MiB)
                    0,  # Average Number of received traffic
                    0,  # Average Number of transmit traffic
                    0,  # Latency
                    0,  # Stall duration
                    0,   # Stall count
                    0, 0, 0, 0, 0, 0, 0, 0, 0, # n_quality (9)
                    0, 0, 0,                   # bitrates (3)
                    0, 0, 0                    # n_sw (3)
                ]), high=np.array([
                    self.max_clients, # Number of clients
                    self.max_delay, # Network delay
                    self.max_pods,  # Number of Pods -- 1)
                    self.max_pods,  # Desired Replicas
                    get_max_cpu(),  # CPU Usage (in m)
                    get_max_mem(),  # MEM Usage (in MiB)
                    get_max_traffic(),  # Average Number of received traffic
                    get_max_traffic(),  # Average Number of transmit traffic
                    get_max_latency(),
                    get_max_stall_duration(),
                    get_max_stall_count(),
                    100, 100, 100, 100, 100, 100, 100, 100, 100, # Max tiles per quality
                    50000, 50000, 50000,                         # Max bitrate (kbps)
                    100, 100, 100                                # Max switches
                ]),
                dtype=np.float32
            )

    # calculates the desired replica count based on a target metric utilization
    def calculate_reward(self):
        # Calculate Number of desired Replicas
        reward = 0
        if self.goal_reward == COST:
            reward = get_cost_reward(self.deploymentList)
        elif self.goal_reward == LATENCY:
            reward = get_latency_reward_online_boutique(ID_VR, self.deploymentList)

        return reward

    def simulation_update(self):
        if self.current_step == 1:
            # Get a random sample!
            sample = self.df.sample()
            # print(sample)

            for i in range(len(DEPLOYMENTS)):
                self.deploymentList[i].num_pods = int(sample[DEPLOYMENTS[i] + '_num_pods'].values[0])
                self.deploymentList[i].num_previous_pods = int(sample[DEPLOYMENTS[i] + '_num_pods'].values[0])

        else:
            pods = []
            previous_pods = []
            diff = []
            for i in range(len(DEPLOYMENTS)):
                pods.append(self.deploymentList[i].num_pods)
                previous_pods.append(self.deploymentList[i].num_previous_pods)
                aux = pods[i] - previous_pods[i]
                diff.append(aux)
                self.df['diff-' + DEPLOYMENTS[i]] = self.df[DEPLOYMENTS[i] + '_num_pods'].diff()

            # print(pods)
            # print(previous_pods)
            # print(diff)
            # print(self.df_aggr)

            data = 0
            for i in range(len(DEPLOYMENTS)):
                data = self.df.loc[self.df[DEPLOYMENTS[i] + '_num_pods'] == pods[i]]
                data = data.loc[data['diff-' + DEPLOYMENTS[i]] == diff[i]]
                if data.size == 0:
                    data = self.df.loc[self.df[DEPLOYMENTS[i] + '_num_pods'] == pods[i]]

            sample = data.sample()
            # print(sample)

        for i in range(len(DEPLOYMENTS)):
            self.deploymentList[i].cpu_usage = int(sample[DEPLOYMENTS[i] + '_cpu_usage'].values[0])
            self.deploymentList[i].mem_usage = int(sample[DEPLOYMENTS[i] + '_mem_usage'].values[0])
            self.deploymentList[i].received_traffic = int(sample[DEPLOYMENTS[i] + '_traffic_in'].values[0])
            self.deploymentList[i].transmit_traffic = int(sample[DEPLOYMENTS[i] + '_traffic_out'].values[0])
            self.deploymentList[i].latency = float("{:.3f}".format(sample[DEPLOYMENTS[i] + '_latency'].values[0]))

        for d in self.deploymentList:
            # Update Desired replicas
            d.update_replicas()
        return

    def save_obs_to_csv(self, obs_file, obs, date, latency):
        file = open(obs_file, 'a+', newline='')  # append
        # file = open(file_name, 'w', newline='') # new
        fields = ["date"]
        for d in self.deploymentList:
            fields.extend([
                d.name + '_num_clients', d.name + '_network_delay', 
                d.name + '_num_pods', d.name + '_desired_replicas',
                d.name + '_cpu_usage', d.name + '_mem_usage', 
                d.name + '_traffic_in', d.name + '_traffic_out'
            ])
            # Add the new client metrics to the CSV header
            for metric in d.client_metrics:
                fields.append(d.name + '_' + metric)

        with file:
            writer = csv.DictWriter(file, fieldnames=fields)
            
            # Build the row dictionary
            row = {
                'date': date,
                'vr-deployment_num_clients': int(obs[0]),
                'vr-deployment_network_delay': int(obs[1]),
                'vr-deployment_num_pods': int(obs[2]),
                'vr-deployment_desired_replicas': int(obs[3]),
                'vr-deployment_cpu_usage': int(obs[4]),
                'vr-deployment_mem_usage': int(obs[5]),
                'vr-deployment_traffic_in': int(obs[6]),
                'vr-deployment_traffic_out': int(obs[7])
            }
            
            # Dynamically add the rest of the metrics from the observation array
            for i, metric in enumerate(self.deploymentList[0].client_metrics):
                # obs[8] is the first new metric (n_720_z1)
                row['vr-deployment_' + metric] = obs[8 + i]
                
            writer.writerow(row)

    def create_csv_file(self, file_name):
        file = open(file_name, 'w', newline='')
        fields = ['date']
        for d in self.deploymentList:
            fields.append(d.name + '_num_clients')
            fields.append(d.name + '_network_delay')
            fields.append(d.name + '_num_pods')
            fields.append(d.name + '_desired_replicas')
            fields.append(d.name + '_cpu_usage')
            fields.append(d.name + '_mem_usage')
            fields.append(d.name + '_traffic_in')
            fields.append(d.name + '_traffic_out')
            
            for metric in d.client_metrics:
                fields.append(d.name + '_' + metric)

        with file:
            writer = csv.DictWriter(file, fieldnames=fields)
            writer.writeheader()  # write header
