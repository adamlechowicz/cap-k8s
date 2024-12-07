import subprocess
import argparse
import time

# arguments are the number of jobs
# use argparse
parser = argparse.ArgumentParser(description='Run experiments on Spark.')
parser.add_argument('--num-jobs', type=int, default=100, help='Number of jobs to submit')
parser.add_argument('--carbon-trace', type=str, default="PJM.csv", help='Carbon trace to use')
parser.add_argument('--job-type', type=str, default="tpch", help='Type of job to run')
args = parser.parse_args()

# check to make sure carbon trace file exists
try:
    with open(args.carbon_trace, "r") as f:
        pass
except FileNotFoundError:
    print(f"Carbon trace file {args.carbon_trace} not found.")
    exit(1)

MODELS = ["default", "cap", "danish", "decima"]
processes = []

def run_experiment(model_name, i):
    global processes
    num_jobs = args.num_jobs
    print(f"Running {num_jobs} jobs with model {model_name}")

    # first, start the flask driver server
    print("Starting flask server...")
    flask_log = open(f"logs/flask_{model_name}.log", "w")
    processes.append(subprocess.Popen(
        ["python3", "/home/cc/flask-driver/test.py", "--model-name", model_name, "--carbon-trace", args.carbon_trace],
        stdout=flask_log,
        stderr=flask_log
    ))

    # start the carbon intensity server if model_name == CAP
    if model_name == "cap":
        print("Starting carbon intensity server...")
        carbon_log = open("logs/carbon_intensity_server.log", "w")
        processes.append(subprocess.Popen(
            ["python3", "/home/cc/carbon-intensity-api-sim/carbonServer.py", "--carbon-trace", args.carbon_trace],
            stdout=carbon_log,
            stderr=carbon_log
        ))

        # wait a few seconds for the carbon intensity server to start
        time.sleep(5)

        print("Starting CAP agent...")
        cap_log = open("logs/cap_agent.log", "w")
        processes.append(subprocess.Popen(
            ["python3", "/home/cc/cap-k8s/cap.py", "--namespace", "spark-ns", "--res-quota-path", "/home/cc/cap-k8s/resource_quota.yaml", "--api-domain", "127.0.0.1:6066", "--min-execs", "20", "--max-execs", "100", "--interval", "60"],
            stdout=cap_log,
            stderr=cap_log
        ))
    
    # run the experiment
    print("Running experiment...")
    exp_log = open(f"logs/experiment_{model_name}.log", "w")
    exp = subprocess.Popen(
        ["python3", "/home/cc/cap-k8s/submit-measure-jobs.py", "--num-jobs", str(num_jobs), "--model-name", model_name, "--carbon-trace", args.carbon_trace, "--tag", f"{i}", "--job-type", args.job_type],
    )

    # wait for the experiment to finish
    exp.wait()

    # once it is done, clean up all the processes
    for p in processes:
        print("Stopping all processes...")
        p.kill()
    print("Killing any outstanding port-forwarding processes...")
    subprocess.run(["ps", "-e", "-f", "|", "grep", "'kubectl port-forward'", "|", "grep", "-v", "grep", "|", "awk", "'{print $2}'", "|", "xargs", "-r", "kill", "-9"], check=True, shell=True)
    
    return
    
if __name__ == "__main__":
    num_to_avg = 1
    for i in range(num_to_avg):
        for model in MODELS:
            run_experiment(model, i)
            # add a delay of 10 seconds between experiments
            print("Sleeping for 10 seconds...")
            time.sleep(10)
            # just to be sure, clear the namespace of all pods again
            print("Deleting all pods in the spark-ns namespace...")
            subprocess.run(["kubectl", "delete", "pods", "--all", "-n", "spark-ns"], check=True)
    print("All experiments completed.")
    exit(0)


