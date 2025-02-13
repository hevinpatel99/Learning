import os
import subprocess
from kafka_streaming_pipeline import file_ingestion, data_validation, data_transformation, data_loading

# List of scripts to run in order
scripts = [
    "/home/dev1070/Hevin_1070/hevin.softvan@gmail.com/projects/Python_Workspace/Learning/data_engineer_task/kafka_streaming_pipeline/file_ingestion.py",
    "/home/dev1070/Hevin_1070/hevin.softvan@gmail.com/projects/Python_Workspace/Learning/data_engineer_task/kafka_streaming_pipeline/data_validation.py",
    "/home/dev1070/Hevin_1070/hevin.softvan@gmail.com/projects/Python_Workspace/Learning/data_engineer_task/kafka_streaming_pipeline/data_transformation.py",
    "/home/dev1070/Hevin_1070/hevin.softvan@gmail.com/projects/Python_Workspace/Learning/data_engineer_task/kafka_streaming_pipeline/data_loading.py"]

for script in scripts:
    script_path = os.path.join(os.path.dirname(__file__), script)
    print(f"Running {script_path}...")
    result = subprocess.run(["python3", script_path], capture_output=True, text=True)
    print(result.stdout)
    print(result.stderr)
