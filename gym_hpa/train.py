import os
from stable_baselines3 import PPO
from stable_baselines3.common.env_checker import check_env
from stable_baselines3.common.callbacks import CheckpointCallback
from sys import argv

# Import the environment
from gym_hpa.envs.vr_application import VrLearning
  
# Usage: python3 -m gym_hpa.train [experiment_name]

if len(argv) > 1:
    experiment_name = argv[1]
else:
    print("Usage: python3 -m gym_hpa.train [experiment_name]")
    print("Example: python3 -m gym_hpa.train [experiment_name] v1")
    exit(1)

print("Initializing VR Autoscaling Environment...")

# Instantiate the environment
env = VrLearning(k8s=False, experiment_name=experiment_name)

# Validate the environment
print("Running Gym API Validation...")
check_env(env, warn=True)
print("Environment validation passed.")

# Define absolute base path 
base_path = "/home/bernardoacp/Documents/Materiais-de-Estudo/vr-learning"

# Saves the model every 500 steps 
checkpoint_callback = CheckpointCallback(
    save_freq=500,
    save_path=os.path.join(base_path, f"models/ppo_checkpoints/{experiment_name}"),
    name_prefix='ppo_vr_model'
)

# Initialize the PPO Agent
model = PPO(
    policy="MlpPolicy",
    env=env,
    learning_rate=3e-4,
    n_steps=100,         
    batch_size=25,       
    gamma=0.99,          # Discount factor for future rewards
    verbose=1,
    tensorboard_log=os.path.join(base_path, f"tensorboard_logs/ppo_vr/{experiment_name}")
)

# Train the Agent and Populate the CSV
total_timesteps = 1000000

print(f"Starting training for {total_timesteps} timesteps...")
print("Note: This will actively scale pods on your Kubernetes cluster.")

model.learn(
    total_timesteps=total_timesteps,
    callback=checkpoint_callback,
    tb_log_name=f"PPO_AbsoluteScaling"
)

# 6. Save the final model
model.save(os.path.join(base_path, "models/ppo_vr_final_{experiment_name}"))
print("Training complete! Offline dataset has been generated.")