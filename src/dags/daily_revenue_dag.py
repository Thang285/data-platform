from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the Spark Application YAML inside Python
spark_job_manifest = {
    "apiVersion": "sparkoperator.k8s.io/v1beta2",
    "kind": "SparkApplication",
    "metadata": {
        "name": "daily-revenue-batch",
        "namespace": "compute"
    },
    "spec": {
        "type": "Python",
        "pythonVersion": "3",
        "mode": "cluster",
        "image": "public.ecr.aws/bitnami/spark:3.5.1-debian-12-r11",
        "imagePullPolicy": "IfNotPresent",
        # 🎯 POINT TO YOUR BATCH SCRIPT HERE
        # We will use a local file copy for simplicity in this step
        "mainApplicationFile": "local:///app/revenue_batch.py",
        "sparkVersion": "3.5.1",
        "restartPolicy": {"type": "Never"},
        
        "sparkConf": {
            "spark.jars.ivy": "/tmp/.ivy2",
            "spark.driver.memoryOverhead": "128m"
        },
        
        "deps": {
            "packages": [
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.262"
            ]
        },
        
        "driver": {
            "cores": 1,
            "coreLimit": "250m",
            "memory": "512m",
            "serviceAccount": "spark-spark-operator-spark",
            "securityContext": {"runAsUser": 0},
            "env": [
                {"name": "AWS_ACCESS_KEY_ID", "value": "minioadmin"},
                {"name": "AWS_SECRET_ACCESS_KEY", "value": "minioadminpassword"},
                {"name": "AWS_REGION", "value": "us-east-1"}
            ],
            # Mount the code (See volumes below)
            "volumeMounts": [{"name": "code-volume", "mountPath": "/app"}]
        },
        "executor": {
            "cores": 1,
            "coreLimit": "250m",
            "instances": 1,
            "memory": "512m",
            "securityContext": {"runAsUser": 0},
            "env": [
                {"name": "AWS_ACCESS_KEY_ID", "value": "minioadmin"},
                {"name": "AWS_SECRET_ACCESS_KEY", "value": "minioadminpassword"}
            ]
        },
        # Create a ConfigMap volume to inject the script
        "volumes": [{"name": "code-volume", "configMap": {"name": "revenue-script-cm"}}],
        
        "hadoopConf": {
            "fs.s3a.endpoint": "http://minio.data.svc.cluster.local:9000",
            "fs.s3a.access.key": "minioadmin",
            "fs.s3a.secret.key": "minioadminpassword",
            "fs.s3a.path.style.access": "true",
            "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "fs.s3a.connection.ssl.enabled": "false",
            "fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        }
    }
}

with DAG(
    'daily_revenue_report',
    default_args=default_args,
    description='Calculates daily revenue from MinIO Parquet files',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False
) as dag:

    submit_batch_job = SparkKubernetesOperator(
        task_id='submit_revenue_calc',
        namespace='compute',
        application_file=spark_job_manifest
    )