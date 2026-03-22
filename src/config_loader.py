"""
config_loader.py
----------------
Loads domain configuration from config.yaml.
This is the ONLY file the system reads for domain-specific settings.
Zero hardcoding in the application layer.
"""
import yaml
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, 'config.yaml')

_config = None

def load_config():
    global _config
    if _config is None:
        with open(CONFIG_PATH, 'r') as f:
            _config = yaml.safe_load(f)
    return _config

def get_active_domain():
    cfg = load_config()
    return cfg.get('domain', 'healthcare')

def get_domain_config():
    cfg = load_config()
    domain = get_active_domain()
    return cfg.get('domains', {}).get(domain, {})

def get_risk_rules():
    """Returns the list of risk rules for the active domain."""
    domain_cfg = get_domain_config()
    return domain_cfg.get('risk_rules', [])

def get_connection_config():
    """Returns the DB/API/File/Cloud connection settings if configured."""
    cfg = load_config()
    return {
        'db_type':       cfg.get('db_type', 'sqlite'),
        'db_host':       cfg.get('db_host'),
        'db_port':       cfg.get('db_port'),
        'db_name':       cfg.get('db_name'),
        'db_user':       cfg.get('db_user'),
        'db_password':   cfg.get('db_password'),
        'api_endpoint':  cfg.get('api_endpoint'),
        'api_token':     cfg.get('api_token'),
        'flat_file_path':cfg.get('flat_file_path'),
        'cloud_provider':cfg.get('cloud_provider'),
        'cloud_bucket':  cfg.get('cloud_bucket'),
        'cloud_key':     cfg.get('cloud_key'),
    }
