"""
Configuration loader for schedule overrides from ConfigMap.
"""

import os
from typing import Any, Dict

import yaml


def load_schedule_config() -> Dict[str, Any]:
    """
    Load schedule configuration from YAML file.

    Returns:
        Dictionary with schedule configuration.
    """
    config_path = os.environ.get(
        'CELERY_SCHEDULE_CONFIG',
        '/etc/poolboy/schedule_config.yaml'
    )
    if os.path.exists(config_path):
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f) or {}
        except yaml.YAMLError as e:
            print(f"Error parsing schedule config YAML: {e}")
            return {}
    return {}


def deep_merge(source: Dict, destination: Dict) -> Dict:
    """
    Recursively merge two dictionaries.

    Args:
        source: Source dictionary to merge from
        destination: Destination dictionary to merge into

    Returns:
        Merged dictionary
    """
    result = destination.copy()
    for key, value in source.items():
        if isinstance(value, dict) and key in result and isinstance(result[key], dict):
            result[key] = deep_merge(value, result[key])
        else:
            result[key] = value
    return result

