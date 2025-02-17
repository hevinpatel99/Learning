"""
Module: pipeline_runner

This script sequentially executes a series of Python scripts in a Kafka streaming
pipeline.

Execution:
- Each script is executed in order using `subprocess.run`.
- Standard output and error logs are captured and printed.

Usage:
Run this script to automate the execution of the entire pipeline.

"""

import os
import subprocess

from data_engineer_task.kafka_streaming_pipeline.config import FILE_PATH

# List of scripts to run in order
scripts = [
    FILE_PATH + "file_ingestion.py",
    FILE_PATH + "data_validation.py",
    FILE_PATH + "data_transformation.py",
    FILE_PATH + "data_loading.py"]

for script in scripts:
    script_path = os.path.join(os.path.dirname(__file__), script)
    print(f"Running {script_path}...")
    result = subprocess.run(["python3", script_path], capture_output=True, text=True)
    print(result.stdout)
    print(result.stderr)
