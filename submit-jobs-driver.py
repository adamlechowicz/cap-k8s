import subprocess
import time

SPARK_SUBMIT_PATH = "../spark/bin/spark-submit"
K8S_CLUSTER_URL = "k8s://https://127.0.0.1:38109"

# Define the Spark submit command template
COMMAND_TEMPLATE = [
    SPARK_SUBMIT_PATH,
    "--master", K8S_CLUSTER_URL,
    "--deploy-mode", "cluster",
    "--name", "spark-pi",
    "--class", "org.apache.spark.examples.SparkPi",
    "--conf", "spark.executor.instances=2",
    "--conf", "spark.kubernetes.container.image=spark:our-own-apache-spark-kb8",
    "--conf", "spark.kubernetes.container.image.pullPolicy=IfNotPresent",
    "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=spark",
    "--conf", "spark.kubernetes.namespace=spark-ns",
    "local:///opt/spark/examples/jars/spark-examples_2.12-3.5.3.jar", "100"
]

def submit_spark_job():
    """
    Start a spark-submit process and return the Popen object.
    """
    process = subprocess.Popen(COMMAND_TEMPLATE)
    return process

def maintain_jobs(target_jobs=20):
    """
    Keep a constant number of Spark jobs running by monitoring subprocesses.
    """
    active_jobs = []  # List of active Popen processes

    while True:
        # Check for completed jobs
        for job in active_jobs[:]:  # Iterate over a copy of the list
            if job.poll() is not None:  # Process has completed
                active_jobs.remove(job)

        # Submit new jobs to maintain the desired count
        while len(active_jobs) < target_jobs:
            print("Active jobs:", len(active_jobs), " Submitting one new job...")
            new_job = submit_spark_job()
            active_jobs.append(new_job)
            time.sleep(1)  # Small delay to avoid overloading

        # Short sleep before re-checking the active jobs
        time.sleep(5)

if __name__ == "__main__":
    maintain_jobs()
