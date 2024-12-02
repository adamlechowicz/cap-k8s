import requests
import subprocess
import yaml
import re
import time
from scipy.special import lambertw
import math
import argparse
import os

# Constants (defaults)
NAMESPACE = "spark-ns"
PATH_TO_RESOURCE_QUOTA = "/Users/adam/GitHub/cap-k8s/resource_quota.yaml"
API_DOMAIN = "http://127.0.0.1:6066"
API_CI_ENDPOINT = API_DOMAIN + "/get_carbon_intensity"
API_REGISTER_ENDPOINT = API_DOMAIN + "/register"
INTERVAL = 1 * 60  # Interval in seconds (15 minutes)

# Validate if the name is a lowercase RFC 1123 subdomain
def is_valid_rfc_1123_subdomain(name):
    pattern = r'^[a-z0-9]([-a-z0-9]*[a-z0-9])?$'
    return re.match(pattern, name) is not None

# Function to check if the namespace exists
def namespace_exists(namespace):
    try:
        result = subprocess.run(
            ["kubectl", "get", "namespace", namespace],
            check=True,
            text=True,
            capture_output=True
        )
        return True
    except subprocess.CalledProcessError:
        return False

# Fetch carbon intensity data from the API
def fetch_carbon_intensity(user_id):
    try:
        # add user_id as a query parameter to the endpoint
        CI_endpoint = f"{API_CI_ENDPOINT}?user_id={user_id}"
        response = requests.get(CI_endpoint, timeout=10)
        response.raise_for_status()
        data = response.json()
        # extract data from the json (carbon_intensity, lower_bound, upper_bound)
        carbon_intensity = data["carbon_intensity"]
        lower_bound = data["lower_bound"]
        upper_bound = data["upper_bound"]
        return carbon_intensity, lower_bound, upper_bound
    except Exception as e:
        print(f"Error fetching carbon intensity data: {e}")
        return None, None, None


# Thresholding logic to determine the number of allowable pods
def calculate_allowable_pods(c_t, L, U, B, driver_pods, K):
    controllable_k = K - B - driver_pods
    if controllable_k <= 0:
        return K
    # increase L slightly to improve responsiveness
    L = L * 1.1
    # solve for alpha (competitive ratio for k-search)
    alpha = 1 / (1 + lambertw( ( (L/U) - 1 ) / math.e ).real )
    thresholds = [ U*(1 - (1 - (1/alpha)) * (1 + (1/(alpha*controllable_k)))**(i-1) ) for i in range(1, controllable_k+1)]
    print(f"Thresholds: {thresholds}")

    # find the first threshold that is greater than the current carbon intensity
    # since the thresholds are decreasing, the index of the first threshold that is less than 
    # the current carbon intensity is the number of allowable pods
    for i, threshold in enumerate(thresholds):
        if threshold < c_t:
            return B + i
    
    return K
    

# Update the Kubernetes resource quota
def update_resource_quota(allowable_execs, args):
    try:
        # Load the existing resource quota definition from the YAML file
        with open(PATH_TO_RESOURCE_QUOTA, "r") as f:
            resource_quota = yaml.safe_load(f)

        # Update the allowable pods
        resource_quota["spec"]["hard"]["cpu"] = str(allowable_execs * 1) # 1 cpu core per exec
        resource_quota["spec"]["hard"]["memory"] = str(allowable_execs * 2) + "Gi" # 1Gi memory per exec

        # Save the updated resource quota definition back to the YAML file
        with open(PATH_TO_RESOURCE_QUOTA, "w") as f:
            yaml.safe_dump(resource_quota, f, default_flow_style=False)

        if not args.testing:
            # Apply the resource quota using kubectl
            subprocess.run(
                ["kubectl", "apply", "-f", PATH_TO_RESOURCE_QUOTA, "-n", NAMESPACE],
                check=True,
                text=True
            )
        print(f"Updated resource quota: {allowable_execs} executors allowed.")
    except subprocess.CalledProcessError as e:
        print(f"Error updating resource quota: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

def get_driver_pods():
    result = subprocess.run(["kubectl", "get", "pods", "-n", "spark-ns"], capture_output=True, text=True)
    output = result.stdout.splitlines()
    driver_pods = 0
    for line in output:
        if "-driver" in line:
            if "Completed" not in line and "Error" not in line:
                driver_pods += 1
    print(f"Current driver pods: {driver_pods}")
    return driver_pods

# Main function to run the script
def main():
    # parse command line arguments
    parser = argparse.ArgumentParser(description="CAP (carbon-aware provisioning) script")
    parser.add_argument("--namespace", required=True, help="Kubernetes namespace")
    parser.add_argument("--res-quota-path", required=True, help="Path to the resource quota YAML file")
    parser.add_argument("--api-domain", required=True, help="Domain for the carbon intensity API")
    parser.add_argument("--min-execs", type=int, default=2, help="Minimum number of executors allowed")
    parser.add_argument("--max-execs", type=int, default=10, help="Maximum number of executors allowed")
    parser.add_argument("--interval", type=int, default=15 * 60, help="Heartbeat interval in seconds")
    parser.add_argument("--testing", type=bool, default=False, help="Testing mode (no kubectl commands)")
    parser.add_argument("--run-once", type=bool, default=False, help="Update the resource quota once and exit (e.g., for using with cron)")

    args = parser.parse_args()

    NAMESPACE = args.namespace
    PATH_TO_RESOURCE_QUOTA = args.res_quota_path
    API_DOMAIN = args.api_domain
    B = args.min_execs
    K = args.max_execs
    INTERVAL = args.interval

    API_CI_ENDPOINT = f"http://{API_DOMAIN}/get_carbon_intensity"
    API_REGISTER_ENDPOINT = f"http://{API_DOMAIN}/register"

    # Check if the namespace exists
    if not namespace_exists(NAMESPACE) and not args.testing:
        print(f"Error: Namespace '{NAMESPACE}' does not exist; check your cluster config.")
        return
    
    # Check if the resource quota file exists
    if not os.path.exists(PATH_TO_RESOURCE_QUOTA):
        print(f"Error: Resource quota file '{PATH_TO_RESOURCE_QUOTA}' does not exist.")
        return
    
    # register with the API
    # first, set the initial timestamp (in ISO format, January 31, 2020 at 6pm)
    timestamp = "2020-01-31T18:00:00"
    # then, register the timestamp with the API
    payload = {"timestamp": timestamp}
    try:
        response = requests.post(API_REGISTER_ENDPOINT, json=payload, timeout=10)
        response.raise_for_status()
        print("Successfully registered with the API.")
    except Exception as e:
        print(f"Error registering with the API: {e}")
        return
    # from the response, we need to extract our unique user_id
    user_id = response.json()["user_id"]
    
    # main loop to fetch carbon data and update resource quota
    while True:
        print("Fetching carbon intensity data...")
        c_t, L, U = fetch_carbon_intensity(user_id)

        # check the current number of driver pods
        driver_pods = get_driver_pods()

        if c_t is not None:
            print(f"Carbon intensity - Current: {c_t}, Forecasted low: {L}, Forecasted high: {U}")
            allowable_pods = calculate_allowable_pods(c_t, L, U, B, driver_pods, K)

            print(f"Calculated allowable pods: {allowable_pods}")
            update_resource_quota(allowable_pods, args)
        else:
            print("Skipping update due to failed data fetch.")

        if args.run_once:
            exit(0)
        else:
            print(f"Sleeping for {INTERVAL // 60} minutes...")
            time.sleep(INTERVAL)

if __name__ == "__main__":
    main()
