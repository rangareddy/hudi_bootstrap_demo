import yaml
import os

# -------------------------------------------------------------------
# YAML Util
# -------------------------------------------------------------------
def load_config(yaml_path: str) -> dict:
    """Load YAML from the given path."""
    if os.path.exists(yaml_path):
        with open(yaml_path) as f:
            return yaml.safe_load(f)
    else:
        raise FileNotFoundError(f"Config file not found: {yaml_path}")    