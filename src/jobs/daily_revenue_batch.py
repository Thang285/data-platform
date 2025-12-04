import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, col, to_date

def main():
    print("🚀 STARTING DAILY REVENUE BATCH JOB...")
    
    spark = SparkSession.builder.appName("DailyRevenueCalc").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Config
    # We read from the Data Lake (MinIO), NOT Kafka
    INPUT_PATH = "s3a://lakehouse/bronze/orders/"
    
    print(f"📂 Reading Data from: {INPUT_PATH}")

    try:
        # 1. Read Parquet (Batch Mode)
        df = spark.read.option("mergeSchema", "true").parquet(INPUT_PATH)

        # 2. Transform: Group by Date and Sum Revenue
        # We assume 'timestamp' is a string, so we cast it to date
        daily_stats = df.withColumn("date", to_date(col("timestamp"))) \
            .groupBy("date") \
            .agg(
                sum("total_amount").alias("total_revenue"),
                count("order_id").alias("total_orders")
            ) \
            .orderBy(col("date").desc())

        # 3. Show Report
        print("📊 DAILY REVENUE REPORT:")
        daily_stats.show()

        # In a real setup, you would write this back to MinIO "Gold" layer:
        # daily_stats.write.mode("overwrite").parquet("s3a://lakehouse/gold/daily_revenue/")
        
    except Exception as e:
        print(f"⚠️ Error reading data (maybe bucket is empty?): {e}")

    print("✅ JOB COMPLETE")
    spark.stop()

if __name__ == "__main__":
    main()