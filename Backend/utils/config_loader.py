# utils/config_loader.py
import yaml
import os

def load_yaml_config(file_path: str) -> dict:
    """
    Safely loads a YAML configuration file.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Config file not found: {file_path}")

    with open(file_path, "r") as f:
        return yaml.safe_load(f) or {}
