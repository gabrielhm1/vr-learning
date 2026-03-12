import logging
import pandas as pd

# Parser to extract and calculate metrics from VR player experiment logs (client-side)

def parse_latency(num_clients, base_path):
    """Calculate average latency across all clients from segment files."""
    results = []
    # Read segment data for each client and extract latency
    for user_id in range(1, int(num_clients) + 1):
        segment_file = f"{base_path}/user_{user_id}/user_{user_id}-segment.csv"
        latency = None
        try:
            with open(segment_file, mode="r") as f:
                df = pd.read_csv(f)
                # Calculate mean download time per segment
                latency = pd.to_numeric(df["Seg_d_time"], errors="coerce").mean()
        except Exception as e:
            logging.error(f"Failed to parse {segment_file}: {e}")

        results.append(latency)

    # Return average latency across all clients
    return sum(results) / len (results)

def parse_qoe_metrics(num_clients, base_path):
    """Calculate Quality of Experience (QoE) metrics for all clients."""
    results = []
    # Parse metrics for each client
    for user_id in range(1, int(num_clients) + 1):
        session_file = f"{base_path}/user_{user_id}/user_{user_id}-session.csv"
        session_data = pd.read_csv(session_file)

        row = session_data.iloc[0]

        # Initialize metrics dictionary for current client
        metrics = {}

        metrics['stall_duration'] = row.get('total_stall', 0)
        metrics['stall_count'] = row.get('stall_count', 0)
        # metrics['start_time'] = row.get('start_time', 0)

        # Get QoE metrics per zone
        for i, zone in enumerate([1, 2, 3]):
            # Extract tiles per zone
            metrics[f'n_720_z{zone}'] = row.get(f'til_720_z{zone}', 0)
            metrics[f'n_1080_z{zone}'] = row.get(f'til_1080_z{zone}', 0)
            metrics[f'n_4k_z{zone}'] = row.get(f'til_4k_z{zone}', 0)

            # Extract switch counts per zone
            metrics[f'n_sw_z{zone}'] = row.get(f'qt_sw_z{zone}', 0)

            # Extract bitrate
            # metrics[f'z{zone}_bit'] = row.get(f'z{zone}_bit', 0)

        # metrics['session_qoe'] = row.get('session_qoe', 0)
        results.append(metrics)

    # Calculate average metrics across all clients
    sums = {}
    for d in results:
        for key, value in d.items():
            sums[key] = sums.get(key, 0) + value

    averages = {k: v/len(results) for k,v in sums.items()}

    # Return averaged QoE metrics
    return averages