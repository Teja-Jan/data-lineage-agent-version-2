"""
run_pipeline.py
===============
The ONLY script you need to run to switch datasets.

Usage:
    1. Edit config.yaml — change 'domain:' to healthcare | retail | finance (or add your own)
    2. Run: python run_pipeline.py
    3. Launch: python -m streamlit run src/app.py

That's it. Zero code changes required.
"""
import os
import sys
import yaml
import importlib
import subprocess

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)
CONFIG_PATH = os.path.join(BASE_DIR, 'config.yaml')

def load_config():
    with open(CONFIG_PATH, 'r') as f:
        return yaml.safe_load(f)

def run_step(label, module_path):
    print(f"\n{'='*60}")
    print(f"  RUNNING: {label}")
    print(f"{'='*60}")
    result = subprocess.run([sys.executable, module_path], capture_output=False)
    if result.returncode != 0:
        print(f"[ERROR] Step failed: {label}")
        sys.exit(1)
    print(f"  DONE: {label}")

def main():
    cfg = load_config()
    domain = cfg.get('domain', 'unknown')
    
    print(f"\n{'#'*60}")
    print(f"  PLUG-AND-PLAY DATA PIPELINE")
    print(f"  Active Domain: {domain.upper()}")
    print(f"  No code changes required.")
    print(f"{'#'*60}")
    
    steps = [
        ("Generate Source Data",        os.path.join(BASE_DIR, 'src', 'data_generation', 'generate_sources.py')),
        ("Initialize Target DW Schema", os.path.join(BASE_DIR, 'src', 'data_generation', 'setup_target_dw.py')),
        ("Seed Lineage & Audit Data",   os.path.join(BASE_DIR, 'src', 'data_generation', 'setup_audit_metadata.py')),
        ("Configure Access Control",    os.path.join(BASE_DIR, 'src', 'data_generation', 'setup_access_control.py')),
        ("Create Reporting Views",      os.path.join(BASE_DIR, 'src', 'data_generation', 'setup_reporting_views.py')),
        ("Run ETL Pipelines (30 days)", os.path.join(BASE_DIR, 'src', 'data_generation', 'run_etl.py')),
    ]
    
    for label, path in steps:
        run_step(label, path)
    
    print(f"\n{'='*60}")
    print(f"  PIPELINE COMPLETE for domain: {domain.upper()}")
    print(f"  Launch the app: python -m streamlit run src/app.py")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()
