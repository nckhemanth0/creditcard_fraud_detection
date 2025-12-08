"""
PHASE B - Spark Streaming Fraud Detection
Complete Flow: Kafka → Spark Streaming → Spark ML → Cassandra → Dashboard

Architecture:
  POS Terminals → Kafka → Spark Streaming → Cassandra → Dashboard
                              ↓
                          Spark ML
                          (Model)
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, year, current_date,
    hour, struct, to_json, when, lit, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType
)
from pyspark.ml import PipelineModel
import os
try:
    from kafka.admin import KafkaAdminClient, NewTopic
except Exception:
    KafkaAdminClient = None
    NewTopic = None

def main():
    print("=" * 60)
    print("PHASE B - Spark Streaming Fraud Detection")
    print("Flow: Kafka → Spark Streaming → ML → Cassandra → Dashboard")
    print("=" * 60)
    
    # Configuration
    KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
    KAFKA_TOPIC = "creditcardTransaction"
    CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
    
    # Initialize Spark Session with Kafka and Cassandra support
    print("\n[1/5] Initializing Spark Streaming...")
    spark = (SparkSession.builder
        .appName("FraudDetection-Streaming")
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
                "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1")
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.cassandra.connection.port", "9042")
        .config("spark.driver.memory", "2g")
        # Force local FS for all streaming/checkpointing
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .config("spark.sql.streaming.checkpointLocation", f"file://{os.path.abspath('/tmp/spark-checkpoint')}")
        .master("local[*]")
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("ERROR")
    print("✓ Spark Streaming Session initialized")

    # Ensure Kafka topic exists (helpful for dev environments)
    def ensure_kafka_topic(broker, topic, partitions=1, replication=1):
        if KafkaAdminClient is None:
            print("⚠ kafka-python not available, skipping topic creation check")
            return False
        try:
            admin = KafkaAdminClient(bootstrap_servers=broker, client_id="topic-creator", request_timeout_ms=10000)
            topics = admin.list_topics()
            if topic in topics:
                print(f"✓ Kafka topic '{topic}' already exists")
                admin.close()
                return True
            else:
                new_topic = NewTopic(name=topic, num_partitions=partitions, replication_factor=replication)
                admin.create_topics([new_topic])
                print(f"✓ Created Kafka topic '{topic}' (partitions={partitions}, replication={replication})")
                admin.close()
                return True
        except Exception as e:
            print(f"⚠ Could not create/verify Kafka topic '{topic}': {e}")
            return False

    # Try to create topic (best-effort, does not abort on failure)
    try:
        ensure_kafka_topic(KAFKA_BROKER, KAFKA_TOPIC, partitions=1, replication=1)
    except Exception as e:
        print(f"⚠ Topic creation helper raised an exception: {e}")
    
    # Define schema for incoming Kafka messages
    transaction_schema = StructType([
        StructField("trans_num", StringType(), True),
        StructField("trans_date_trans_time", StringType(), True),
        StructField("cc_num", StringType(), True),
        StructField("merchant", StringType(), True),
        StructField("category", StringType(), True),
        StructField("amt", DoubleType(), True),
        StructField("first", StringType(), True),
        StructField("last", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zip", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("long", DoubleType(), True),
        StructField("city_pop", IntegerType(), True),
        StructField("job", StringType(), True),
        StructField("dob", StringType(), True),
        StructField("merch_lat", DoubleType(), True),
        StructField("merch_long", DoubleType(), True),
        StructField("is_fraud", IntegerType(), True)
    ])
    
    # Load trained Spark ML model
    print("\n[2/5] Loading Spark ML model...")
    try:
        model = PipelineModel.load("models/spark_fraud_model")
        print("✓ Spark ML model loaded (Random Forest)")
        use_ml_model = True
    except Exception as e:
        print(f"⚠ Model not found, using is_fraud label directly: {e}")
        use_ml_model = False
    
    # Create streaming DataFrame from Kafka
    print(f"\n[3/5] Connecting to Kafka...")
    print(f"  Broker: {KAFKA_BROKER}")
    print(f"  Topic: {KAFKA_TOPIC}")
    
    kafka_df = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load())
    
    print("✓ Connected to Kafka stream")
    
    # Parse JSON from Kafka value
    parsed_df = kafka_df \
        .selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), transaction_schema).alias("data")) \
        .select("data.*")
    
    # Feature Engineering (same as training)
    print("\n[4/5] Setting up feature engineering pipeline...")
    processed_df = parsed_df \
        .withColumn("trans_time", to_timestamp(col("trans_date_trans_time"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("dob_ts", to_timestamp(col("dob"), "yyyy-MM-dd")) \
        .withColumn("age", (year(current_date()) - year(col("dob_ts"))).cast("double")) \
        .withColumn("distance", 
            ((col("lat") - col("merch_lat")) ** 2 + (col("long") - col("merch_long")) ** 2) ** 0.5) \
        .withColumn("trans_hour", hour(col("trans_time")).cast("double")) \
        .withColumn("amt", col("amt").cast("double")) \
        .withColumn("city_pop", col("city_pop").cast("double"))
    
    # Function to process each batch and save to Cassandra
    def process_batch(batch_df, batch_id):
        if batch_df.count() == 0:
            return
            
        print(f"\n📦 Processing batch {batch_id} with {batch_df.count()} transactions...")
        
        try:
            if use_ml_model:
                # Apply ML model for prediction
                predictions = model.transform(batch_df)
                fraud_alerts = predictions.filter(col("prediction") == 1.0).select(
                    col("cc_num"),
                    col("trans_num"),
                    col("trans_time"),
                    col("merchant"),
                    col("category"),
                    col("amt"),
                    lit(1.0).alias("is_fraud"),
                    col("probability").getItem(1).alias("prediction_score")
                )
            else:
                # Use is_fraud label directly
                fraud_alerts = batch_df.filter(col("is_fraud") == 1).select(
                    col("cc_num"),
                    col("trans_num"),
                    col("trans_time"),
                    col("merchant"),
                    col("category"),
                    col("amt"),
                    col("is_fraud").cast("double"),
                    lit(0.95).alias("prediction_score")
                )
            
            fraud_count = fraud_alerts.count()
            if fraud_count > 0:
                print(f"  🚨 FRAUD DETECTED: {fraud_count} transactions!")
                
                # Show fraud alerts
                fraud_alerts.select("trans_num", "cc_num", "amt", "merchant").show(5, truncate=False)
                
                # Save to Cassandra
                try:
                    fraud_alerts.write \
                        .format("org.apache.spark.sql.cassandra") \
                        .options(table="fraud_alert", keyspace="creditcard") \
                        .mode("append") \
                        .save()
                    print(f"  ✓ Saved {fraud_count} alerts to Cassandra")
                except Exception as e:
                    print(f"  ⚠ Cassandra write failed: {e}")
            else:
                print(f"  ✓ No fraud in this batch")
                
        except Exception as e:
            print(f"  ⚠ Batch processing error: {e}")
    
    # Start streaming query
    print("\n[5/5] Starting Spark Streaming query...")
    print("=" * 60)
    print("🔴 STREAMING ACTIVE - Waiting for Kafka transactions...")
    print("   Run './run.sh producer' in another terminal to send transactions")
    print("   Press Ctrl+C to stop")
    print("=" * 60)
    
    query = processed_df \
        .writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .trigger(processingTime="5 seconds") \
        .start()
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n\nStopping Spark Streaming...")
        query.stop()
    
    print("\n" + "=" * 60)
    print("Spark Streaming stopped.")
    print("=" * 60)
    
    spark.stop()

if __name__ == "__main__":
    main()
