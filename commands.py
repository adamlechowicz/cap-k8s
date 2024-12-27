def get_command_template(model):
    max_execs = 8          # max diff for good carbon traces (CAISO, DE, ON)
    # max_execs = 5        # med diff for medium carbon traces (DE, NSW, PJM)
    #max_execs = 6         # smallest diff for bad carbon traces (PJM, NSW, ZA)
    if model == "cap" or model == "danish":
        max_execs = 6

    SPARK_SUBMIT_PATH = "/home/cc/cap-k8s/spark/bin/spark-submit"
    K8S_CLUSTER_URL = "k8s://https://127.0.0.1:6443"

    # Define the Spark submit command template
    SPARKTC_COMMAND_TEMPLATE = [
        SPARK_SUBMIT_PATH,
        "--master", K8S_CLUSTER_URL,
        "--deploy-mode", "cluster",
        "--name", "spark-pr",
        "--class", "org.apache.spark.examples.SparkTC",
        "--conf", "spark.kubernetes.container.image=alechowicz/spark:spark-k8s",
        "--conf", "spark.kubernetes.container.image.pullPolicy=IfNotPresent",
        "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=spark",
        "--conf", "spark.executor.cores=3",
        "--conf", "spark.executor.memory=7g",
        "--conf", "spark.dynamicAllocation.enabled=True",
        "--conf", "spark.dynamicAllocation.shuffleTracking.enabled=True",
        "--conf", f"spark.dynamicAllocation.maxExecutors={max_execs}",
        "--conf", "spark.kubernetes.namespace=spark-ns",
        "local:///opt/spark/examples/jars/spark-examples_2.12-3.5.3.jar",
    ]
    TPCH_BASE_COMMAND_TEMPLATE = [
        SPARK_SUBMIT_PATH,
        "--master", K8S_CLUSTER_URL,
        "--deploy-mode", "cluster",
        "--class", "main.scala.TpchQuery",
        "--name", "tpch",
        "--conf", "spark.kubernetes.container.image=alechowicz/spark:spark-k8s",
        "--conf", "spark.kubernetes.container.image.pullPolicy=IfNotPresent",
        "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=spark",
        "--conf", "spark.executor.cores=3",
        "--conf", "spark.executor.memory=7g",
        "--conf", "spark.dynamicAllocation.enabled=True",
        "--conf", "spark.dynamicAllocation.shuffleTracking.enabled=True",
        "--conf", f"spark.dynamicAllocation.maxExecutors={max_execs}",
        "--conf", "spark.kubernetes.namespace=spark-ns",
    ]
    ALIBABA_BASE_COMMAND_TEMPLATE = [
        SPARK_SUBMIT_PATH,
        "--master", K8S_CLUSTER_URL,
        "--deploy-mode", "cluster",
        "--class", "org.apache.spark.examples.SparkPi",
        "--name", "alibaba",
        "--conf", "spark.kubernetes.container.image=alechowicz/spark-py:spark-k8s",
        "--conf", "spark.kubernetes.container.image.pullPolicy=IfNotPresent",
        "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=spark",
        "--conf", "spark.executor.cores=3",
        "--conf", "spark.executor.memory=7g",
        "--conf", "spark.dynamicAllocation.enabled=True",
        "--conf", "spark.dynamicAllocation.shuffleTracking.enabled=True",
        "--conf", f"spark.dynamicAllocation.maxExecutors={max_execs}",
        "--conf", "spark.kubernetes.namespace=spark-ns",
    ]

    # Define the TPCH 100M command template
    TPCH_100M_COMMAND_TEMPLATE = TPCH_BASE_COMMAND_TEMPLATE.copy()
    TPCH_100M_COMMAND_TEMPLATE.append("local:///opt/spark/examples/jars/100m-tpc-h-queries_2.12-1.0.jar")

    # Define the TPCH 1G command template
    TPCH_1G_COMMAND_TEMPLATE = TPCH_BASE_COMMAND_TEMPLATE.copy()
    TPCH_1G_COMMAND_TEMPLATE.append("local:///opt/spark/examples/jars/1g-tpc-h-queries_2.12-1.0.jar")

    # Define the TPCH 10G command template
    TPCH_10G_COMMAND_TEMPLATE = TPCH_BASE_COMMAND_TEMPLATE.copy()
    TPCH_10G_COMMAND_TEMPLATE.append("local:///opt/spark/examples/jars/10g-tpc-h-queries_2.12-1.0.jar")

    COMMAND_TEMPLATES = {
        "sparktc": SPARKTC_COMMAND_TEMPLATE,
        "tpch100m": TPCH_100M_COMMAND_TEMPLATE,
        "tpch1g": TPCH_1G_COMMAND_TEMPLATE,
        "tpch10g": TPCH_10G_COMMAND_TEMPLATE,
    }

    for i in range(1, 101):
        COMMAND_TEMPLATES[f"alibaba{i}"] = ALIBABA_BASE_COMMAND_TEMPLATE.copy()
        COMMAND_TEMPLATES[f"alibaba{i}"].append(f"local:///opt/spark/examples/job{i}.py")

    return COMMAND_TEMPLATES