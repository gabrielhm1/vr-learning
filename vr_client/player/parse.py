import logging
import pandas as pd

# Parser to extract and calculate metrics from VR player experiment logs (client-side)

def parse_latency(num_clients, base_path):
    """Calculate average latency across all clients from segment files."""
    results = []
    # Read segment data for each client and extract latency
    for user_id in range(1, int(num_clients) + 1):
        segment_file = f"{base_path}/user_{user_id}/user_{user_id}-segment.csv"
        try:
            with open(segment_file, mode="r") as f:
                df = pd.read_csv(f)
                # Calculate mean download time per segment
                latency = pd.to_numeric(df[" Seg_d_time"], errors="coerce").mean()
        except Exception as e:
            logging.error(f"Failed to parse {segment_file}: {e}")

        results.append(latency)

    # Return average latency across all clients
    return sum(results) / len (results)

def parse_qoe(num_clients, base_path):
    """Calculate Quality of Experience (QoE) metrics for all clients."""
    # QoE calculation parameters
    mu = 4.3  # stall penalty
    lamda = 1  # zone switch penalty
    omega = 4.3  # startup delay penalty
    alphas = [0.7, 0.2, 0.1]  # zone weights

    results = []
    # Parse metrics for each client
    for user_id in range(1, int(num_clients) + 1):
        segment_file = f"{base_path}/user_{user_id}/user_{user_id}-segment.csv"
        session_file = f"{base_path}/user_{user_id}/user_{user_id}-session.csv"

        segment_data = pd.read_csv(segment_file)
        session_data = pd.read_csv(session_file)

        # Map column names to correct zone designations
        column_mapping = {
        " z1_bit": " til_4k_z3",
        " z2_bit": " z1_bit",
        " z3_bit": " z2_bit",
        " til_4k_z3": " z3_bit"
        }

        session_data.rename(columns=column_mapping, inplace=True)

        # Initialize metrics dictionary for current client
        metrics = {}
        metrics['total_stall'] = session_data.iloc[0][' total_stall']
        metrics['start_time'] = session_data.iloc[0][' start_time']
        
        # Calculate QoE per zone
        qoe = 0
        for i in range(0, 3):
            # Extract bitrate and switch counts per zone
            metrics[f'z{i+1}_bit'] = segment_data.groupby('Zone')[' Bitrate'].sum()[f'Z{i+1}']
            metrics[f'qt_sw_z{i+1}'] = session_data.iloc[0][f' qt_sw_z{i+1}']

            # QoE formula: bitrate - penalties for stalls, switches, and startup
            per_zone = metrics[f'z{i+1}_bit'] - (mu * metrics['total_stall']) - (lamda * metrics[f'qt_sw_z{i+1}']) - (omega * metrics['start_time'])

            qoe += alphas[i] * per_zone

        metrics['overall_qoe'] = qoe
        results.append(metrics)

    # Calculate average metrics across all clients
    sums = {}
    for d in results:
        for key, value in d.items():
            sums[key] = sums.get(key, 0) + value

    averages = {k: v/len(results) for k,v in sums.items()}

    # Return averaged QoE metrics
    return averages