# Define the Spark submit command template
SPARKTC_COMMAND_TEMPLATE = [
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
    "--conf", "spark.dynamicAllocation.maxExecutors=4",
    "--conf", "spark.kubernetes.namespace=spark-ns",
    "local:///opt/spark/examples/jars/spark-examples_2.12-3.5.3.jar",
]
COMMAND_TEMPLATES = {
    "sparktc": SPARKTC_COMMAND_TEMPLATE
}