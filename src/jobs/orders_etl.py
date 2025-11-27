import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

def main():
    print("🚀 CI/CD SPARK JOB STARTED...")
    
    spark = SparkSession.builder.appName("CICD_Orders_ETL").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Config (Internal DNS)
    KAFKA = "kafka-controller-0.kafka-controller-headless.ingestion.svc.cluster.local:9092"
    BUCKET = "lakehouse"

    # 1. Read
    print(f"🔌 Connecting to Kafka: {KAFKA}")
    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA) \
        .option("subscribe", "orders") \
        .option("startingOffsets", "earliest").load()

    # 2. Schema
    schema = StructType().add("order_id", StringType()).add("user_id", IntegerType()) \
        .add("product", StringType()).add("total_amount", DoubleType()).add("timestamp", StringType())

    parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # 3. Write to MinIO
    # Note: We use the S3A protocol
    query = parsed_df.writeStream.format("parquet") \
        .option("path", f"s3a://{BUCKET}/bronze/orders/") \
        .option("checkpointLocation", f"s3a://{BUCKET}/checkpoints/orders_cicd/") \
        .outputMode("append").start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
