import os
from stable_baselines3 import PPO
from stable_baselines3.common.env_checker import check_env
from stable_baselines3.common.callbacks import CheckpointCallback

# Import the environment
from gym_hpa.envs.vr_application import VrLearning

def main():
    print("Initializing VR Autoscaling Environment...")
    
    # Instantiate the environment
    env = VrLearning(k8s=True)
    
    # Validate the environment
    print("Running Gym API Validation...")
    check_env(env, warn=True)
    print("Environment validation passed.")

    # Saves the model every 500 steps 
    checkpoint_callback = CheckpointCallback(
        save_freq=500,
        save_path='../models/ppo_checkpoints/',
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
        tensorboard_log="../tensorboard_logs/ppo_vr/"
    )

    # Train the Agent and Populate the CSV
    total_timesteps = 2500 
    
    print(f"Starting training for {total_timesteps} timesteps...")
    print("Note: This will actively scale pods on your Kubernetes cluster.")
    
    model.learn(
        total_timesteps=total_timesteps,
        callback=checkpoint_callback,
        tb_log_name="PPO_AbsoluteScaling_Run1"
    )

    # 6. Save the final model
    model.save("../models/ppo_vr_final")
    print("Training complete! Offline dataset has been generated.")

if __name__ == "__main__":
    main()