"""
PHASE A - Step 1: Initial Data Import using Apache Spark SQL
Uses PySpark for distributed ETL and loads data into Cassandra
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, year, current_date, 
    sqrt, pow as spark_pow, lit, when
)
from pyspark.sql.types import DoubleType
import math

# Haversine distance UDF for calculating distance between customer and merchant
def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate distance in km between two coordinates"""
    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
        return 0.0
    R = 6371  # Earth's radius in km
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)
    a = math.sin(delta_lat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

def main():
    print("=" * 60)
    print("PHASE A - Step 1: Spark SQL Data Import to Cassandra")
    print("=" * 60)
    
    # Initialize Spark Session with Cassandra connector
    print("\n[1/7] Initializing Apache Spark...")
    spark = SparkSession.builder \
        .appName("FraudDetection-DataImport") \
        .config("spark.cassandra.connection.host", "localhost") \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
        .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print("✓ Spark Session initialized")
    print(f"  Spark Version: {spark.version}")
    
    # Register UDF for distance calculation
    from pyspark.sql.functions import udf
    distance_udf = udf(haversine_distance, DoubleType())
    
    # Load CSV data using Spark SQL
    print("\n[2/7] Loading dataset with Spark SQL...")
    train_df = spark.read.csv("dataset/fraudTrain.csv", header=True, inferSchema=True)
    test_df = spark.read.csv("dataset/fraudTest.csv", header=True, inferSchema=True)
    
    # Union both datasets
    transactions_df = train_df.union(test_df)
    total_records = transactions_df.count()
    print(f"✓ Loaded {total_records:,} transactions using Spark DataFrame")
    
    # Show schema
    print("\n[3/7] Spark DataFrame Schema:")
    transactions_df.printSchema()
    
    # ETL: Clean and transform data using Spark SQL
    print("\n[4/7] Performing distributed ETL with Spark SQL...")
    
    # Drop unnamed column if exists
    if "Unnamed: 0" in transactions_df.columns:
        transactions_df = transactions_df.drop("Unnamed: 0")
    
    # Parse timestamp
    transactions_df = transactions_df.withColumn(
        "trans_time", 
        to_timestamp(col("trans_date_trans_time"), "yyyy-MM-dd HH:mm:ss")
    )
    
    # Parse DOB and calculate age
    transactions_df = transactions_df.withColumn(
        "dob_parsed",
        to_timestamp(col("dob"), "yyyy-MM-dd")
    )
    transactions_df = transactions_df.withColumn(
        "age",
        (year(current_date()) - year(col("dob_parsed"))).cast("int")
    )
    
    # Calculate distance using UDF
    transactions_df = transactions_df.withColumn(
        "distance",
        distance_udf(col("lat"), col("long"), col("merch_lat"), col("merch_long"))
    )
    
    print("✓ ETL transformations complete")
    
    # Extract unique customers using Spark SQL
    print("\n[5/7] Extracting customers using Spark SQL...")
    transactions_df.createOrReplaceTempView("transactions")
    
    customers_df = spark.sql("""
        SELECT DISTINCT 
            cc_num, first, last, gender, street, city, state, 
            CAST(zip AS STRING) as zip, lat, long, job, 
            TO_TIMESTAMP(dob, 'yyyy-MM-dd') as dob
        FROM transactions
    """)
    
    customer_count = customers_df.count()
    print(f"✓ Extracted {customer_count:,} unique customers")
    
    # Analyze fraud distribution using Spark SQL
    print("\n[6/7] Analyzing fraud distribution with Spark SQL...")
    fraud_stats = spark.sql("""
        SELECT 
            is_fraud,
            COUNT(*) as count,
            ROUND(AVG(amt), 2) as avg_amount,
            ROUND(MAX(amt), 2) as max_amount
        FROM transactions
        GROUP BY is_fraud
        ORDER BY is_fraud
    """)
    fraud_stats.show()
    
    # Prepare DataFrames for Cassandra
    print("\n[7/7] Saving to Cassandra using Spark Cassandra Connector...")
    
    # Select columns for fraud and non-fraud transactions
    fraud_transactions = transactions_df.filter(col("is_fraud") == 1).select(
        col("cc_num").cast("string"),
        col("trans_time"),
        col("trans_num"),
        col("category"),
        col("merchant"),
        col("amt").cast("double"),
        col("merch_lat").cast("double"),
        col("merch_long").cast("double"),
        col("distance").cast("double"),
        col("age").cast("int"),
        col("is_fraud").cast("double")
    )
    
    non_fraud_transactions = transactions_df.filter(col("is_fraud") == 0).select(
        col("cc_num").cast("string"),
        col("trans_time"),
        col("trans_num"),
        col("category"),
        col("merchant"),
        col("amt").cast("double"),
        col("merch_lat").cast("double"),
        col("merch_long").cast("double"),
        col("distance").cast("double"),
        col("age").cast("int"),
        col("is_fraud").cast("double")
    )
    
    fraud_count = fraud_transactions.count()
    non_fraud_count = non_fraud_transactions.count()
    
    print(f"  Fraud transactions: {fraud_count:,}")
    print(f"  Non-fraud transactions: {non_fraud_count:,}")
    
    # Write to Cassandra using Spark Cassandra Connector
    try:
        customers_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="customer", keyspace="creditcard") \
            .mode("append") \
            .save()
        print("✓ Customers saved to Cassandra")
        
        fraud_transactions.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="fraud_transaction", keyspace="creditcard") \
            .mode("append") \
            .save()
        print("✓ Fraud transactions saved to Cassandra")
        
        non_fraud_transactions.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="non_fraud_transaction", keyspace="creditcard") \
            .mode("append") \
            .save()
        print("✓ Non-fraud transactions saved to Cassandra")
        
    except Exception as e:
        print(f"Note: Cassandra write skipped (run in Docker for full integration): {e}")
        # Save to parquet as fallback for local testing
        fraud_transactions.write.mode("overwrite").parquet("data/fraud_transactions.parquet")
        non_fraud_transactions.write.mode("overwrite").parquet("data/non_fraud_transactions.parquet")
        customers_df.write.mode("overwrite").parquet("data/customers.parquet")
        print("✓ Data saved to Parquet files (Spark native format)")
    
    print("\n" + "=" * 60)
    print("✓ SPARK SQL DATA IMPORT COMPLETE!")
    print(f"  Total Transactions: {total_records:,}")
    print(f"  Fraud: {fraud_count:,} | Non-Fraud: {non_fraud_count:,}")
    print("=" * 60)
    
    spark.stop()

if __name__ == "__main__":
    main()

