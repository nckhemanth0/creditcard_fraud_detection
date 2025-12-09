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
from py4j.protocol import Py4JJavaError
import math
from pathlib import Path
import os


# =============================================================
# Haversine distance UDF
# =============================================================
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
    
    # ---------------------------------------------------------
    # [1/7] Initialize Spark Session with Cassandra connector
    # ---------------------------------------------------------
    print("\n[1/7] Initializing Apache Spark...")

    spark = (
        SparkSession.builder
        .appName("FraudDetection-DataImport")
        .master("local[*]")

        # FIX: Prevent Spark from trying to talk to HDFS (port 9000)
        .config("spark.hadoop.fs.defaultFS", "file:///")

        # Cassandra connector
        .config("spark.cassandra.connection.host", "localhost")
        .config("spark.cassandra.connection.port", "9042")
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1")
        .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")

        # Memory
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    print("✓ Spark Session initialized")
    print(f"  Spark Version: {spark.version}")

    # Register UDF
    from pyspark.sql.functions import udf
    distance_udf = udf(haversine_distance, DoubleType())
    
    # ---------------------------------------------------------
    # [2/7] Load CSV data
    # ---------------------------------------------------------
    print("\n[2/7] Loading dataset with Spark SQL...")

    # Choose CSV directory: prefer `dataset/` (original layout) then `data/` (alternate)
    def csv_path(fname: str) -> str:
        for prefix in ("dataset", "data"):
            p = os.path.join(prefix, fname)
            if os.path.exists(p):
                return p
        # fallback to given relative path (will raise later)
        return os.path.join("data", fname)

    train_csv = csv_path("fraudTrain.csv")
    test_csv = csv_path("fraudTest.csv")

    train_df = spark.read.csv(train_csv, header=True, inferSchema=True)
    test_df = spark.read.csv(test_csv, header=True, inferSchema=True)

    # FIX: Remove accidental empty column if present (ensure reassignment)
    if "_c0" in train_df.columns:
        train_df = train_df.drop("_c0")
    if "_c0" in test_df.columns:
        test_df = test_df.drop("_c0")

    # Union datasets
    transactions_df = train_df.union(test_df)
    total_records = transactions_df.count()

    print(f"✓ Loaded {total_records:,} transactions using Spark DataFrame")

    # ---------------------------------------------------------
    # [3/7] Show schema
    # ---------------------------------------------------------
    print("\n[3/7] Spark DataFrame Schema:")
    transactions_df.printSchema()
    
    # ---------------------------------------------------------
    # [4/7] Distributed ETL
    # ---------------------------------------------------------
    print("\n[4/7] Performing distributed ETL with Spark SQL...")

    # Drop unnamed index column
    if "Unnamed: 0" in transactions_df.columns:
        transactions_df = transactions_df.drop("Unnamed: 0")
    
    # Parse timestamps
    transactions_df = transactions_df.withColumn(
        "trans_time",
        to_timestamp(col("trans_date_trans_time"), "yyyy-MM-dd HH:mm:ss")
    )

    transactions_df = transactions_df.withColumn(
        "dob_parsed",
        to_timestamp(col("dob"), "yyyy-MM-dd")
    )

    transactions_df = transactions_df.withColumn(
        "age",
        (year(current_date()) - year(col("dob_parsed"))).cast("int")
    )

    # Distance calculation
    transactions_df = transactions_df.withColumn(
        "distance",
        distance_udf(col("lat"), col("long"), col("merch_lat"), col("merch_long"))
    )

    print("✓ ETL transformations complete")
    
    # ---------------------------------------------------------
    # [5/7] Extract customers
    # ---------------------------------------------------------
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
    
    # ---------------------------------------------------------
    # [6/7] Fraud distribution
    # ---------------------------------------------------------
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
    
    # ---------------------------------------------------------
    # [7/7] Save to Cassandra or Parquet fallback
    # ---------------------------------------------------------
    print("\n[7/7] Saving to Cassandra using Spark Cassandra Connector...")

    fraud_transactions = transactions_df.filter(col("is_fraud") == 1).select(
        col("cc_num").cast("string"),
        "trans_time",
        "trans_num",
        "category",
        "merchant",
        col("amt").cast("double"),
        col("merch_lat").cast("double"),
        col("merch_long").cast("double"),
        col("distance").cast("double"),
        col("age").cast("int"),
        col("is_fraud").cast("double")
    )

    non_fraud_transactions = transactions_df.filter(col("is_fraud") == 0).select(
        col("cc_num").cast("string"),
        "trans_time",
        "trans_num",
        "category",
        "merchant",
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

    # Safe, chunked write wrapper to avoid overwhelming Cassandra.
    def safe_write(df, table, chunk_size=50000, max_retries=5, max_chunks=200):
        total = df.count()
        if total == 0:
            print(f"✓ No rows to write for {table}")
            return

        # If small enough, write in one shot
        if total <= chunk_size:
            try:
                df.repartition(4).write \
                    .format("org.apache.spark.sql.cassandra") \
                    .options(table=table, keyspace="creditcard") \
                    .mode("append") \
                    .save()
                print(f"✓ Saved to Cassandra table: {table} ({total:,} rows)")
                return
            except Exception as e:
                print(f"⚠ Cassandra write failed for {table}: {e}")
                print(f"⚠ Falling back to Parquet for {table}")
                Path("data").mkdir(exist_ok=True)
                df.write.mode("overwrite").parquet(f"data/{table}.parquet")
                print(f"✓ Parquet saved: data/{table}.parquet")
                return

        # Large dataset: split into sequential chunks using randomSplit.
        # Cap the number of chunks via `max_chunks` to avoid too many tiny writes.
        num_splits = int(math.ceil(total / float(chunk_size)))
        num_splits = max(1, min(num_splits, max_chunks))
        print(f"→ Writing {total:,} rows to {table} in {num_splits} sequential chunk(s) (max_chunks={max_chunks})")
        fractions = [1.0 / num_splits] * num_splits
        splits = df.randomSplit(fractions, seed=42)

        written = 0
        for idx, split_df in enumerate(splits, start=1):
            if split_df.count() == 0:
                continue
            attempt = 0
            while attempt < max_retries:
                try:
                    rows = split_df.count()
                    split_df.repartition(4).write \
                        .format("org.apache.spark.sql.cassandra") \
                        .options(table=table, keyspace="creditcard") \
                        .mode("append") \
                        .save()
                    written += rows
                    print(f"  ✓ Chunk {idx}/{num_splits} written ({rows:,} rows)")
                    break
                except Exception as e:
                    attempt += 1
                    wait = attempt * 5
                    print(f"  ⚠ Chunk {idx} write failed (attempt {attempt}/{max_retries}): {e}")
                    print(f"    Retrying in {wait}s...")
                    import time
                    time.sleep(wait)
            else:
                # Failed after retries: bail out and save Parquet fallback
                print(f"⚠ Failed to write chunk {idx} after {max_retries} attempts. Saving Parquet fallback for {table}")
                Path("data").mkdir(exist_ok=True)
                df.write.mode("overwrite").parquet(f"data/{table}.parquet")
                print(f"✓ Parquet saved: data/{table}.parquet")
                return

        print(f"✓ Completed write to {table}: requested {total:,} | written {written:,}")

    safe_write(customers_df, "customer")
    safe_write(fraud_transactions, "fraud_transaction")
    # Write non-fraud in a limited number of chunks (8-16 recommended).
    # Use `max_chunks=16` to keep the number of sequential writes bounded.
    safe_write(non_fraud_transactions, "non_fraud_transaction", max_chunks=16)
    
    print("\n" + "=" * 60)
    print("✓ SPARK SQL DATA IMPORT COMPLETE!")
    print(f"  Total Transactions: {total_records:,}")
    print(f"  Fraud: {fraud_count:,} | Non-Fraud: {non_fraud_count:,}")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
