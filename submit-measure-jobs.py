import subprocess
import csv
import pandas as pd
from datetime import datetime, timedelta
import time
import argparse
from tqdm import tqdm
import pytz
import os

utc = pytz.UTC

SPARK_SUBMIT_PATH = "/home/cc/cap-k8s/spark/bin/spark-submit"
K8S_CLUSTER_URL = "k8s://https://127.0.0.1:6443"

# Global dictionary to store job start and end times
job_times = {}
job_carbon_footprint = {}
executor_tracking = {}
job_driver_pods = {}
completed_jobs = 0

# carbon accounting 
INITIAL_DATETIME = datetime.fromisoformat("2022-01-31T22:00:00")
ACTUAL_DATETIME = datetime.now()

# Parse command line arguments
parser = argparse.ArgumentParser(description='Submit Spark jobs and track carbon footprint.')
parser.add_argument('--num-jobs', type=int, default=100, help='Number of jobs to submit')
parser.add_argument('--model-name', type=str, default="default", help='Name of scheduler to use')
parser.add_argument('--target-running-jobs', type=int, default=2, help='Target number of running jobs')
parser.add_argument('--carbon-trace', type=str, default="PJM.csv", help='Carbon trace to use')
parser.add_argument('--job-type', type=str, default="sparktc", help='Type of job to run')
parser.add_argument('--tag', type=str, default="", help='Tag for the experiment')
args = parser.parse_args()

NUM_JOBS = args.num_jobs
TARGET_RUNNING_JOBS = args.target_running_jobs
MODEL_NAME = args.model_name
data_file_path = args.carbon_trace
job_type = args.job_type
tag = args.tag

pbar = tqdm(total=NUM_JOBS)  # Set the total number of iterations if known

# Load the carbon intensity data
carbon_data = pd.read_csv(data_file_path)
carbon_data['datetime'] = pd.to_datetime(carbon_data['datetime'])  # Ensure timestamps are datetime objects
# make the datetime column the index
carbon_data.set_index('datetime', inplace=True)

# Define the Spark submit command template
COMMAND_TEMPLATE = [
    SPARK_SUBMIT_PATH,
    "--master", K8S_CLUSTER_URL,
    "--deploy-mode", "cluster",
    "--name", "spark-pr",
    "--class", "org.apache.spark.examples.SparkTC",
    "--conf", "spark.kubernetes.container.image=alechowicz/spark:spark-k8b",
    "--conf", "spark.kubernetes.container.image.pullPolicy=IfNotPresent",
    "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=spark",
    "--conf", "spark.executor.cores=6",
    "--conf", "spark.executor.memory=7g",
    "--conf", "spark.dynamicAllocation.enabled=True",
    "--conf", "spark.dynamicAllocation.shuffleTracking.enabled=True",
    "--conf", "spark.dynamicAllocation.maxExecutors=20",
    "--conf", "spark.kubernetes.namespace=spark-ns",
    "local:///opt/spark/examples/jars/spark-examples_2.12-3.5.3.jar",
]


def submit_spark_job(job_id):
    """
    Submit a Spark job and return the process handle.
    """
    log_file = open(f"logs/job_{job_id}.log", "w")
    process = subprocess.Popen(
        COMMAND_TEMPLATE,
        stdout=log_file,
        stderr=log_file
    )
    # Log the start time
    job_times[job_id] = {'start_time': datetime.now(), 'end_time': None}
    job_carbon_footprint[job_id] = 0.0
    executor_tracking[job_id] = []
    return process

def get_pods():
    result = subprocess.run(["kubectl", "get", "pods", "-n", "spark-ns"], capture_output=True, text=True)
    return result.stdout.splitlines()

def describe_pod(pod_name):
    result = subprocess.run(["kubectl", "describe", "pod", pod_name, "-n", "spark-ns"], capture_output=True, text=True)
    return result.stdout

def identify_driver_pod(job_id):
    log_file = f"logs/job_{job_id}.log"
    with open(log_file, 'r') as file:
        for line in file:
            if "submission ID" in line:
                driver_pod = line.split("submission ID spark-ns:")[1].split("-driver")[0] + "-driver"
                job_driver_pods[driver_pod] = job_id
                print(f"Identified driver pod for job {job_id}: {driver_pod}")
                return driver_pod
    return None

def get_carbon_intensity():
    global ACTUAL_DATETIME, INITIAL_DATETIME
    # Calculate the time delta
    current_datetime = datetime.now()
    time_delta = current_datetime - ACTUAL_DATETIME

    # actual real time
    # elapsed_hours = int(time_delta.total_seconds() // 3600)

    # sped up by a factor of 60 (1 minute in real time = 1 hour in simulation time)
    elapsed_hours = int(time_delta.total_seconds() // 60)

    # Determine the corresponding row in the carbon intensity data
    # Determine the corresponding row in the carbon intensity data
    carbon_time = (INITIAL_DATETIME + timedelta(hours=elapsed_hours)).replace(tzinfo=utc)
    # if the carbon time is beyond the last time in the data, reset the ACTUAL_DATETIME (so that we loop back to the beginning of the data)
    if carbon_time > carbon_data.index[-1]:
        ACTUAL_DATETIME = current_datetime
        INITIAL_DATETIME = carbon_data.index[0]
        carbon_time = INITIAL_DATETIME
        elapsed_hours = 0
    rounded_time = carbon_time.replace(minute=0, second=0, microsecond=0)  # Round down to the nearest hour
    # convert to iso format
    rounded_time = rounded_time.isoformat()

    # Retrieve the intensity value
    # note that rounded_time is a datetime object so we can index the carbon_data DataFrame with it
    row = carbon_data.loc[rounded_time]
    carbon_intensity = row['carbon_intensity_avg']

    return carbon_intensity

def maintain_jobs(target_jobs=TARGET_RUNNING_JOBS):
    """
    Keep a constant number of Spark jobs running by monitoring subprocesses.
    """
    global completed_jobs, executor_tracking, job_driver_pods, job_times, job_carbon_footprint
    active_jobs = {}  # Dict of active Popen processes
    i = 0  # Job ID counter
    original_target_jobs = target_jobs

    while completed_jobs < NUM_JOBS:
        # Check for completed jobs
        for job_id, job in list(active_jobs.items()):  # Iterate over a copy of the dict items
            if job_id not in job_driver_pods.values():
                pod_name = identify_driver_pod(job_id)
            if job.poll() is not None:  # Process has completed
                # look to see whether the job completed successfully
                if job.returncode != 0:
                    # implement a backoff strategy -- if jobs are failing, decrement target_jobs
                    target_jobs = max(1, target_jobs - 1)
                    del active_jobs[job_id]
                    del job_times[job_id]
                    del job_carbon_footprint[job_id]
                else: 
                    completed_jobs += 1
                    # if jobs are completing successfully, increment target_jobs (up to the original value)
                    target_jobs = min(original_target_jobs, target_jobs + 1)
                    pbar.update(1)  # Update the progress bar manually
                    job_times[job_id]['end_time'] = datetime.now()  # Log the end time
                    del active_jobs[job_id]

        # Submit new jobs to maintain the desired count
        while len(active_jobs) < target_jobs:
            print("Active jobs:", len(active_jobs), " Submitting one new job...")
            new_job = submit_spark_job(i)
            active_jobs[i] = new_job
            i += 1
            time.sleep(0.5)  # Small delay to avoid overloading
        
        pods = get_pods()
        for pod in pods:
            if "exec" in pod and "Running" in pod:
                pod_name = pod.split()[0]
                description = describe_pod(pod_name)
                for line in description.splitlines():
                    if "Controlled By" in line:
                        driver_pod = line.split("Pod/")[1]
                        if driver_pod in job_driver_pods.keys():
                            job_id = job_driver_pods[driver_pod]
                            executor_tracking[job_id].append((datetime.now().strftime("%M:%S"), pod_name))
                            job_carbon_footprint[job_id] += get_carbon_intensity()*(1/60)
                        else:
                            print("Driver pod not found in job_driver_pods") 

        # Short sleep before re-checking the active jobs
        time.sleep(1)

    if completed_jobs >= NUM_JOBS:
        print("All jobs have completed.")
        print("Writing logs to CSV...")
        write_log_to_csv()     
        pbar.close()  # Close the progress bar                                   

def write_log_to_csv():
    # get the current datetime in iso format
    current_datetime = datetime.now().isoformat()

    # want to save the file in a folder results/{MODEL_NAME}/{JOB_TYPE}_{NUM_JOBS}/times_{tag}.csv
    # folder results and results/{MODEL_NAME} should already exist
    # JOB_TYPE_NUM_JOBS folder should be created if it doesn't exist, do that first
    try:
        os.makedirs(f"results/{MODEL_NAME}/{job_type}_{NUM_JOBS}")
    except FileExistsError:
        pass
    
    filename = f"results/{MODEL_NAME}/{job_type}_{NUM_JOBS}/times_{tag}.csv"
    with open(filename, 'w', newline='') as csvfile:
        fieldnames = ['job_id', 'start_time', 'end_time', 'carbon_footprint', 'executors']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for job_id, times in job_times.items():
            writer.writerow({
                'job_id': job_id,
                'start_time': times['start_time'],
                'end_time': times['end_time'],
                'carbon_footprint': job_carbon_footprint.get(job_id, 0),
                'executors': executor_tracking.get(job_id, [])
            })
    # after I write the log to a csv, I should run "kubectl delete pods --all -n spark-ns" to clean up the pods
    # that were created by the spark jobs
    print("Deleting all pods in the spark-ns namespace...")
    subprocess.run(["kubectl", "delete", "pods", "--all", "-n", "spark-ns"], check=True)

if __name__ == "__main__":
    try:
        maintain_jobs()
    except KeyboardInterrupt:
        write_log_to_csv()
        pbar.close()  # Close the progress bar
