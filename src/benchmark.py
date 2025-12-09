"""
BIG DATA BENCHMARK: Spark vs Pandas

Saves a JSON + human-readable report under `reports/` with performance metrics.
Run: `python src/benchmark.py`
"""
import time
import os
import json
from datetime import datetime


def csv_path(fname: str) -> str:
    for prefix in ("dataset", "data"):
        p = os.path.join(prefix, fname)
        if os.path.exists(p):
            return p
    return os.path.join("dataset", fname)


def benchmark_pandas(train_path, test_path):
    import pandas as pd
    import math

    start = time.time()

    load_start = time.time()
    train_df = pd.read_csv(train_path)
    test_df = pd.read_csv(test_path)
    df = pd.concat([train_df, test_df], ignore_index=True)
    load_time = time.time() - load_start

    etl_start = time.time()
    df['trans_time'] = pd.to_datetime(df['trans_date_trans_time'])
    df['age'] = datetime.utcnow().year - pd.to_datetime(df['dob']).dt.year
    # Euclidean approx (not haversine) for quick benchmarking
    df['distance'] = ((df['lat'] - df['merch_lat']) ** 2 + (df['long'] - df['merch_long']) ** 2) ** 0.5
    df['trans_hour'] = df['trans_time'].dt.hour

    fraud_count = int((df['is_fraud'] == 1).sum())
    _ = df.groupby('category')['amt'].mean()
    etl_time = time.time() - etl_start

    total_time = time.time() - start
    row_count = len(df)

    return {
        'tool': 'Pandas',
        'rows': row_count,
        'load_time': load_time,
        'etl_time': etl_time,
        'total_time': total_time,
        'records_per_sec': row_count / total_time if total_time > 0 else None,
        'fraud_count': fraud_count,
    }


def benchmark_spark(train_path, test_path):
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, to_timestamp, year, current_date, hour

    spark = SparkSession.builder \
        .appName("Benchmark") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    start = time.time()
    load_start = time.time()
    train_df = spark.read.csv(train_path, header=True, inferSchema=True)
    test_df = spark.read.csv(test_path, header=True, inferSchema=True)
    df = train_df.union(test_df)
    row_count = df.count()
    load_time = time.time() - load_start

    etl_start = time.time()
    df = df.withColumn("trans_time", to_timestamp(col("trans_date_trans_time"), "yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("age", year(current_date()) - year(to_timestamp(col("dob"), "yyyy-MM-dd")))
    df = df.withColumn("distance", ((col("lat") - col("merch_lat")) ** 2 + (col("long") - col("merch_long")) ** 2) ** 0.5)
    df = df.withColumn("trans_hour", hour(col("trans_time")))

    fraud_count = df.filter(col("is_fraud") == 1).count()
    _ = df.groupBy("category").avg("amt").collect()
    etl_time = time.time() - etl_start

    total_time = time.time() - start

    spark.stop()

    return {
        'tool': 'Spark',
        'rows': row_count,
        'load_time': load_time,
        'etl_time': etl_time,
        'total_time': total_time,
        'records_per_sec': row_count / total_time if total_time > 0 else None,
        'fraud_count': fraud_count,
    }


def main():
    train_path = csv_path('fraudTrain.csv')
    test_path = csv_path('fraudTest.csv')

    if not os.path.exists(train_path) or not os.path.exists(test_path):
        print('Error: dataset CSVs not found in dataset/ or data/.')
        return

    reports_dir = 'reports'
    os.makedirs(reports_dir, exist_ok=True)

    print('Running Pandas benchmark...')
    pandas_results = benchmark_pandas(train_path, test_path)
    print('Running Spark benchmark...')
    spark_results = benchmark_spark(train_path, test_path)

    timestamp = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    report = {
        'timestamp': timestamp,
        'pandas': pandas_results,
        'spark': spark_results,
    }

    json_path = os.path.join(reports_dir, f'benchmark_{timestamp}.json')
    txt_path = os.path.join(reports_dir, f'benchmark_{timestamp}.txt')

    with open(json_path, 'w') as fh:
        json.dump(report, fh, indent=2)

    with open(txt_path, 'w') as fh:
        fh.write('BIG DATA BENCHMARK - Spark vs Pandas\n')
        fh.write('=' * 60 + '\n')
        fh.write(f"Pandas: rows={pandas_results['rows']:,} total_time={pandas_results['total_time']:.2f}s records/sec={pandas_results['records_per_sec']:.0f}\n")
        fh.write(f"Spark:  rows={spark_results['rows']:,} total_time={spark_results['total_time']:.2f}s records/sec={spark_results['records_per_sec']:.0f}\n")
        fh.write('\nDetailed JSON saved to: ' + json_path + '\n')

    print('\nBenchmark complete. Reports saved:')
    print(' -', json_path)
    print(' -', txt_path)

    print('\nEvaluation guidance:')
    print(' - Compare total_time and records_per_sec')
    print(' - Spark should show higher records/sec on multi-core hosts')
    print(' - For large-scale runs, measure memory and GC; run Spark with more executors')


if __name__ == '__main__':
    main()
