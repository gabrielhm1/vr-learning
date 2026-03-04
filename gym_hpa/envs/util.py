import csv
import math

def get_stall_penalty(deployment_list):
    # Scaling factors
    scale_duration = 20.0  # seconds
    scale_count = 10.0      # number of stalls

    # Extract client metrics
    for d in deployment_list:
        stall_duration = d.stall_duration
        stall_count = d.stall_count

    # Calculate bounded penalty
    p_qoe = math.tanh((stall_duration / scale_duration) + (stall_count / scale_count))

    return p_qoe

def get_cost_penalty(deployment_list):
    for d in deployment_list:
        cpu_usage = d.cpu_usage
        cpu_limit = d.cpu_limit 

    # Calculate utilization and ensure it doesn't exceed 1.0
    utilization = min(1.0, cpu_usage / cpu_limit)
    
    # Penalty is the inverse of utilization
    p_cost = 1.0 - utilization

    return p_cost


def get_qoe_reward(deployment_list):
    alpha = 0.7 # stall weight
    beta = 0.3 # cost weight

    p_stall = get_stall_penalty(deployment_list)
    p_cost = get_cost_penalty(deployment_list)
    reward = - ((alpha * p_stall) + (beta * p_cost))

    return reward

def get_num_pods(deployment_list):
    n = 0
    for d in deployment_list:
        n += d.num_pods

    return n
