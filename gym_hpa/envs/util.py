import csv


def save_obs_to_csv(file_name, timestamp, num_pods, desired_replicas, cpu_usage, mem_usage,
                    traffic_in, traffic_out, latency, lstm_1_step, lstm_5_step):
    file = open(file_name, 'a+', newline='')  # append
    # file = open(file_name, 'w', newline='') # new
    with file:
        fields = ['date', 'num_pods', 'cpu', 'mem', 'desired_replicas',
                  'traffic_in', 'traffic_out', 'latency', 'lstm_1_step', 'lstm_5_step']
        writer = csv.DictWriter(file, fieldnames=fields)
        # writer.writeheader() # write header
        writer.writerow(
            {'date': timestamp,
             'num_pods': int("{}".format(num_pods)),
             'cpu': int("{}".format(cpu_usage)),
             'mem': int("{}".format(mem_usage)),
             'desired_replicas': int("{}".format(desired_replicas)),
             'traffic_in': int("{}".format(traffic_in)),
             'traffic_out': int("{}".format(traffic_out)),
             'latency': float("{:.3f}".format(latency)),
             'lstm_1_step': int("{}".format(lstm_1_step)),
             'lstm_5_step': int("{}".format(lstm_5_step))}
        )


def save_to_csv(file_name, episode, avg_pods, avg_latency, reward, execution_time):
    file = open(file_name, 'a+', newline='')  # append
    # file = open(file_name, 'w', newline='')
    with file:
        fields = ['episode', 'avg_pods', 'avg_latency', 'reward', 'execution_time']
        writer = csv.DictWriter(file, fieldnames=fields)
        # writer.writeheader()
        writer.writerow(
            {'episode': episode,
             'avg_pods': float("{:.2f}".format(avg_pods)),
             'avg_latency': float("{:.4f}".format(avg_latency)),
             'reward': float("{:.2f}".format(reward)),
             'execution_time': float("{:.2f}".format(execution_time))}
        )


def get_stall_penalty(deployment_list):
    # Scaling factors
    scale_duration = 10.0  # seconds
    scale_count = 5.0      # number of stalls

    # Extract pod metrics
    for d in deployment_list:
        stall_duration = d.stall_duration
        stall_count = d.stall_count

    # Calculate soft-scaled penalty (unbounded, continuous)
    p_qoe = (stall_duration / scale_duration) + (stall_count / scale_count)

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
    reward = - ((alpha * p_cost) + (beta * p_stall))

    return reward

def get_num_pods(deployment_list):
    n = 0
    for d in deployment_list:
        n += d.num_pods

    return n
