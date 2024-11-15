import requests
import subprocess
import json
import time

# Constants
NAMESPACE = "spark-ns"
RESOURCE_QUOTA_NAME = "spark-quota"
API_ENDPOINT = "https://example.com/carbon-intensity"  # Replace with actual endpoint
B = 2  # Minimum number of executors (pods)
K = 10  # Maximum number of executors (pods)
INTERVAL = 15 * 60  # Interval in seconds (15 minutes)

# Fetch carbon intensity data from the API
def fetch_carbon_intensity():
    try:
        response = requests.get(API_ENDPOINT, timeout=10)
        response.raise_for_status()
        data = response.json()
        return {
            "current": data["current"],
            "lower_bound": data["lower_bound"],
            "upper_bound": data["upper_bound"],
        }
    except Exception as e:
        print(f"Error fetching carbon intensity data: {e}")
        return None

# Thresholding logic to determine the number of allowable pods
def calculate_allowable_pods(current, lower_bound, upper_bound):
    # Placeholder logic
    # Example: Use a linear scaling between B and K based on current intensity
    # Replace this with your actual thresholding scheme
    if current < lower_bound:
        return K
    elif current > upper_bound:
        return B
    else:
        scale = (current - lower_bound) / (upper_bound - lower_bound)
        return max(B, min(K, int(B + (K - B) * (1 - scale))))

# Update the Kubernetes resource quota
def update_resource_quota(allowable_pods):
    resource_quota = {
        "apiVersion": "v1",
        "kind": "ResourceQuota",
        "metadata": {
            "name": RESOURCE_QUOTA_NAME,
            "namespace": NAMESPACE,
        },
        "spec": {
            "hard": {
                "pods": str(allowable_pods),
            }
        }
    }

    try:
        # Save the resource quota definition to a temporary file
        with open("resource_quota.json", "w") as f:
            json.dump(resource_quota, f, indent=4)

        # Apply the resource quota using kubectl
        subprocess.run(
            ["kubectl", "apply", "-f", "resource_quota.json"],
            check=True,
            text=True
        )
        print(f"Updated resource quota: {allowable_pods} pods allowed.")
    except subprocess.CalledProcessError as e:
        print(f"Error updating resource quota: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        # Cleanup temporary file
        try:
            subprocess.run(["rm", "resource_quota.json"], check=True)
        except Exception as cleanup_error:
            print(f"Error cleaning up temporary file: {cleanup_error}")

# Main function to run the script
def main():
    while True:
        print("Fetching carbon intensity data...")
        intensity_data = fetch_carbon_intensity()

        if intensity_data:
            current = intensity_data["current"]
            lower_bound = intensity_data["lower_bound"]
            upper_bound = intensity_data["upper_bound"]

            print(f"Carbon intensity - Current: {current}, Lower: {lower_bound}, Upper: {upper_bound}")
            allowable_pods = calculate_allowable_pods(current, lower_bound, upper_bound)

            print(f"Calculated allowable pods: {allowable_pods}")
            update_resource_quota(allowable_pods)
        else:
            print("Skipping update due to failed data fetch.")

        print(f"Sleeping for {INTERVAL // 60} minutes...")
        time.sleep(INTERVAL)

if __name__ == "__main__":
    main()
