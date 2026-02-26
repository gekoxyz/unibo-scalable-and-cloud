# Earthquake Co-occurrence Analysis on Cloud Dataproc

This project automates the execution of a Scalable Spark job on Google Cloud Dataproc to analyze earthquake co-occurrence data. It uses a Python control script to handle infrastructure management (cluster creation/deletion), Scala compilation, and performance benchmarking.

## Prerequisites

- **Google Cloud SDK** (`gcloud`, `gsutil`) installed and authenticated.
- **Python 3.x**
- **SBT** (Scala Build Tool)
- **Java JDK 8 or 11** (Compatible with Spark/Scala 2.12)

## 1. Configuration

Open `run.py` and modify the global variables at the top of the file to match your GCP project details:

```python
PROJECT_ID = "your-project-id"             # e.g., earthquake-analysis
REGION = "europe-west1"                    # e.g., us-central1
BUCKET_NAME = "your-bucket-name"           # e.g., earthquake-data-bucket
CLUSTER_NAME = "earthquake-analysis"
```

## 2. One-Time Setup

Before running the benchmark for the first time, execute the following commands in your terminal to prepare your GCP environment (datasets, buckets, and permissions).
Replace variables with your specific project details.
```Bash
# 1. Login to Google Cloud
gcloud auth login

# 2. Create Project (Skip if you already have one)
gcloud projects create YOUR_PROJECT_ID --name="Earthquake Analysis"

# 3. Link Billing
gcloud billing projects link YOUR_PROJECT_ID --billing-account=YOUR_BILLING_ID

# 4. Create Cloud Storage Bucket
gsutil mb -p YOUR_PROJECT_ID -l europe-west1 gs://YOUR_BUCKET_NAME

# 5. Upload Dataset
gsutil cp Datasets/dataset-earthquakes-full.csv gs://YOUR_BUCKET_NAME/dataset/dataset-earthquakes-full.csv

# 6. Enable APIs
gcloud services enable dataproc.googleapis.com --project=YOUR_PROJECT_ID
gcloud services enable cloudresourcemanager.googleapis.com --project=YOUR_PROJECT_ID

# 7. Grant IAM Permissions (Required for the cluster to access the bucket)
# Find your compute service account email (usually: [project-number]-compute@developer.gserviceaccount.com)
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID --member=serviceAccount:YOUR_SERVICE_ACCOUNT_EMAIL --role=roles/storage.objectAdmin
```

## 3. Usage

The `run.py` script manages the entire workflow. It will:

- Compile the Scala code (sbt package).
- Upload the JAR to the bucket.
- Loop through defined worker and partition configurations.
- Create a Dataproc cluster, run the job, download results, and delete the cluster.

To start the benchmark:
```Bash
python run.py
```

## 4. Output

- Benchmark Metrics: A file named `benchmark_results_8-256.csv` will be generated in the root directory, containing execution times and status for every configuration.
- Job Data: The actual Spark output (Top Pair and Frequency) is downloaded to the `local_results/` directory.