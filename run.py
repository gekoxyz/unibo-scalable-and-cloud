"""
commands to run one time only
gcloud projects create mgaliazzo-scalable-and-cloud --name="Earthquake Analysis"
gcloud billing projects link mgaliazzo-scalable-and-cloud --billing-account=01AD8E-5E6D0B-24B066
gsutil mb -p mgaliazzo-scalable-and-cloud -l europe-west1 gs://mgaliazzo-scalable-and-cloud-data
gsutil cp Datasets/dataset-earthquakes-full.csv gs://mgaliazzo-scalable-and-cloud-data/dataset/dataset-earthquakes-full.csv
gcloud services enable dataproc.googleapis.com --project=mgaliazzo-scalable-and-cloud
gcloud services enable cloudresourcemanager.googleapis.com --project=mgaliazzo-scalable-and-cloud
gcloud projects add-iam-policy-binding mgaliazzo-scalable-and-cloud \
    --member=serviceAccount:437993739493-compute@developer.gserviceaccount.com \
    --role=roles/storage.objectAdmin
"""

# to check clusters:
# gcloud dataproc clusters list --region=europe-west1 --project=mgaliazzo-scalable-and-cloud

import subprocess
import time
import datetime
import csv

# --- Configuration ---
PROJECT_ID = "mgaliazzo-scalable-and-cloud"
REGION = "europe-west1"
BUCKET_NAME = "mgaliazzo-scalable-and-cloud-data" 
CLUSTER_NAME = "earthquake-analysis"

# JAR Configuration
JAR_NAME = "earthquakes_2.12-1.0.jar"
LOCAL_JAR_PATH = f"target/scala-2.12/{JAR_NAME}"
REMOTE_JAR_PATH = f"gs://{BUCKET_NAME}/{JAR_NAME}"

# Data Paths
INPUT_DATA = f"gs://{BUCKET_NAME}/dataset/dataset-earthquakes-full.csv" # Using full dataset [cite: 9]
BASE_OUTPUT_PATH = f"gs://{BUCKET_NAME}/results"

# 2 workers = 8 cores, 3 workers = 12 cores, 4 workers = 16 cores.
WORKER_CONFIGS = [2, 3, 4] 
PARTITION_CONFIGS = [4, 8, 12, 16]

RESULTS_FILE = "benchmark_results.csv"

def run_command(cmd):
  print(f"\n[EXEC] {cmd}")
  subprocess.check_call(cmd, shell=True)

def create_cluster(num_workers):
  print(f"--- Creating Cluster with {num_workers} workers ---")
  cmd = f"""
  gcloud dataproc clusters create {CLUSTER_NAME} \
    --project={PROJECT_ID} \
    --region={REGION} \
    --num-workers={num_workers} \
    --master-boot-disk-size=100GB \
    --worker-boot-disk-size=100GB \
    --master-machine-type=n2-standard-4 \
    --worker-machine-type=n2-standard-4 \
  """
  run_command(cmd)

def delete_cluster():
  print(f"--- Deleting Cluster ---")
  cmd = f"gcloud dataproc clusters delete {CLUSTER_NAME} --region={REGION} --project={PROJECT_ID} --quiet"
  run_command(cmd)

def run_spark_job(num_partitions, unique_run_id):
    output_path = f"{BASE_OUTPUT_PATH}/{unique_run_id}"
    spark_args = f"{INPUT_DATA} {output_path} {num_partitions}"

    cmd = f"""
    gcloud dataproc jobs submit spark \
      --cluster={CLUSTER_NAME} \
      --project={PROJECT_ID} \
      --region={REGION} \
      --jar={REMOTE_JAR_PATH} \
      -- {spark_args}
    """
    
    start_time = time.time()
    run_command(cmd)
    end_time = time.time()

    local_output_dir = f"local_results/{unique_run_id}"
    print(f"Downloading results to {local_output_dir}...")
    subprocess.call(f"mkdir -p {local_output_dir}", shell=True)
    download_cmd = f"gsutil cp -r {BASE_OUTPUT_PATH}/{unique_run_id}/* {local_output_dir}"
    run_command(download_cmd)
    
    return end_time - start_time


if __name__ == "__main__":
  print("--- Preparing Environment ---")
  run_command("sbt package")
  run_command(f"gsutil cp {LOCAL_JAR_PATH} {REMOTE_JAR_PATH}")
  
  with open(RESULTS_FILE, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["Workers", "Partitions", "Duration_Seconds", "Timestamp", "Status"])

  for workers in WORKER_CONFIGS:
    try:
      create_cluster(workers)

      for partitions in PARTITION_CONFIGS:
        print(f"\n>>> TESTING: Workers={workers}, Partitions={partitions} <<<")
        
        run_id = f"w{workers}_p{partitions}_{int(time.time())}"
        
        try:
          duration = run_spark_job(partitions, run_id)
          print(f">>> SUCCESS: Duration = {duration:.2f} seconds")
          
          with open(RESULTS_FILE, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([workers, partitions, f"{duration:.2f}", datetime.datetime.now(), "Success"])
                
        except Exception as e:
          print(f">>> FAILED: Workers={workers}, Partitions={partitions}")
          with open(RESULTS_FILE, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([workers, partitions, "0", datetime.datetime.now(), "Failed"])

    except Exception as e:
      print(f"[CRITICAL] Failed to create cluster with {workers} workers. Moving to next config.")
      print(e)
    
    finally:
      delete_cluster()

  print(f"\n--- Benchmarking Complete. Results saved to {RESULTS_FILE} ---")