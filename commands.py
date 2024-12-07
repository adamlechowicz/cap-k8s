SPARK_SUBMIT_PATH = "/home/cc/cap-k8s/spark/bin/spark-submit"
K8S_CLUSTER_URL = "k8s://https://127.0.0.1:6443"

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
    "--conf", "spark.dynamicAllocation.maxExecutors=8",
    "--conf", "spark.kubernetes.namespace=spark-ns",
    "local:///opt/spark/examples/jars/spark-examples_2.12-3.5.3.jar",
]
TPCH_BASE_COMMAND_TEMPLATE = [
    SPARK_SUBMIT_PATH,
    "--master", K8S_CLUSTER_URL,
    "--deploy-mode", "cluster",
    "--name", "tpch",
    "--class", "main.scala.TpchQuery",
    "--conf", "spark.kubernetes.container.image=alechowicz/spark:spark-k8b",
    "--conf", "spark.kubernetes.container.image.pullPolicy=IfNotPresent",
    "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=spark",
    "--conf", "spark.executor.cores=6",
    "--conf", "spark.executor.memory=7g",
    "--conf", "spark.dynamicAllocation.enabled=True",
    "--conf", "spark.dynamicAllocation.shuffleTracking.enabled=True",
    "--conf", "spark.dynamicAllocation.maxExecutors=8",
    "--conf", "spark.kubernetes.namespace=spark-ns",
]

# Define the TPCH 100M command template
TPCH_100M_COMMAND_TEMPLATE = TPCH_BASE_COMMAND_TEMPLATE.copy() + ["local:///opt/spark/examples/jars/100m-tpc-h-queries_2.12-1.0.jar "]

# Define the TPCH 1G command template
TPCH_1G_COMMAND_TEMPLATE = TPCH_BASE_COMMAND_TEMPLATE.copy() + ["local:///opt/spark/examples/jars/1g-tpc-h-queries_2.12-1.0.jar "]

# Define the TPCH 10G command template
TPCH_10G_COMMAND_TEMPLATE = TPCH_BASE_COMMAND_TEMPLATE.copy() + ["local:///opt/spark/examples/jars/10g-tpc-h-queries_2.12-1.0.jar "]

COMMAND_TEMPLATES = {
    "sparktc": SPARKTC_COMMAND_TEMPLATE,
    "tpch100m": TPCH_100M_COMMAND_TEMPLATE,
    "tpch1g": TPCH_1G_COMMAND_TEMPLATE,
    "tpch10g": TPCH_10G_COMMAND_TEMPLATE,
}