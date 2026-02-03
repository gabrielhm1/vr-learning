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

def parse_qoe(num_clients, session_duration, base_path):
    """Calculate Quality of Experience (QoE) metrics for all clients."""
    # --- QoE calculation parameters ---
    # Weights for resolutions
    w_4k = 5.00
    w_1080 = 3.33
    w_720 = 1.67

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
        row = session_data.iloc[0]

        # Initialize metrics dictionary for current client
        metrics = {}

        metrics['total_stall'] = session_data.iloc[0][' total_stall']
        metrics['start_time'] = session_data.iloc[0][' start_time']

        if session_duration > 0:
            metrics['stall_term'] = metrics['total_stall'] / session_duration
        else:
            metrics['stall_term'] = 0
        
        # Calculate QoE per zone
        total_score = 0
        n_chunks = segment_data[' Seg_no'].max()
        for i, zone in enumerate([1, 2, 3]):
            # Extract tiles per zone
            n_720 = row.get(f' til_720_z{zone}', 0)
            n_1080 = row.get(f' til_1080_z{zone}', 0)
            n_4k = row.get(f' til_4k_z{zone}', 0)

            metrics[f'n_720_z{zone}'] = n_720
            metrics[f'n_1080_z{zone}'] = n_1080
            metrics[f'n_4k_z{zone}'] = n_4k

            n_tiles = n_720 + n_1080 + n_4k

            # Extract switch counts per zone
            n_switches = row.get(f' qt_sw_z{zone}')

            if n_tiles > 0:
                q_res = (w_720 * n_720 + w_1080 * n_1080 + w_4k * n_4k) / n_tiles
            else:
                q_res = 0

            if n_chunks > 0:
                q_sw = n_switches / n_chunks
            else:
                q_sw = 0

            per_zone = q_res - lamda * q_sw
            total_score += alphas[i] * per_zone

            # Save relevant metrics 
            metrics[f'n_sw_z{zone}'] = n_switches
            metrics[f'res_term_z{zone}'] = q_res
            metrics[f'sw_term_z{zone}'] = q_sw

        qoe = total_score - mu * metrics['stall_term'] - omega * metrics['start_time']

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