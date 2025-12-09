"""
Robust Spark ML training script
Prefer Cassandra as the primary source for training data, normalize schema,
impute categorical modes (no 'unknown'), dynamically build pipeline,
write a report and save the model locally.
"""
import os
from pathlib import Path
from datetime import datetime

from pyspark.sql import SparkSession
import time
from pyspark.sql.functions import (
    col, to_timestamp, year, current_date, hour, lit, when
)
from pyspark.sql.types import DoubleType

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator


def main():
    HDFS_URI = os.environ.get("HDFS_URI")
    CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "localhost")

    print("\n" + "=" * 60)
    print("Spark ML Training — Cassandra-first, robust preprocessing")
    print("=" * 60 + "\n")

    spark_builder = (
        SparkSession.builder
        .appName("FraudDetection-ML-Training")
        .master("local[*]")
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.cassandra.connection.port", "9042")
        # include the Spark Cassandra connector on the driver/executor classpath
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1")
        .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        # Reduce Cassandra write strictness and connector batch size to avoid timeouts
        .config("spark.cassandra.output.consistency.level", "LOCAL_ONE")
        .config("spark.cassandra.output.batch.size.rows", "128")
        .config("spark.cassandra.connection.timeout_ms", "10000")
        # Reduce shuffle partitions to limit concurrent writer tasks when reading/writing
        .config("spark.sql.shuffle.partitions", "8")
    )

    spark = spark_builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # ------------------------------------------------------------------
    # 1) Load data (Cassandra first)
    # ------------------------------------------------------------------
    df = None
    cassandra_ok = False
    try:
        fraud_df = (
            spark.read.format("org.apache.spark.sql.cassandra")
                .options(table="fraud_transaction", keyspace="creditcard")
                .load()
        )
        non_fraud_df = (
            spark.read.format("org.apache.spark.sql.cassandra")
                .options(table="non_fraud_transaction", keyspace="creditcard")
                .load()
        )

        try:
            if not fraud_df.rdd.isEmpty():
                cassandra_ok = True
        except Exception:
            try:
                if fraud_df.count() > 0:
                    cassandra_ok = True
            except Exception:
                cassandra_ok = False

        if cassandra_ok:
            print("✓ Cassandra tables found — loading training data from Cassandra")
            df = fraud_df.union(non_fraud_df)

            # Snapshot to HDFS/local to avoid re-querying Cassandra repeatedly
            try:
                df = df.cache()
                _ = df.count()
                if HDFS_URI:
                    snapshot = os.path.join(HDFS_URI.rstrip('/'), "data/cassandra_snapshot.parquet")
                else:
                    snapshot = "data/cassandra_snapshot.parquet"
                    Path("data").mkdir(parents=True, exist_ok=True)

                df.write.mode("overwrite").parquet(snapshot)
                df = spark.read.parquet(snapshot)
                print(f"✓ Wrote snapshot to {snapshot}")
            except Exception as e:
                print(f"⚠ Warning: failed to snapshot Cassandra DataFrame: {e}")

    except Exception as e:
        print(f"⚠ Cassandra read error: {e}")

    # If Cassandra not available, try HDFS/local snapshots and finally CSVs
    if df is None:
        print("→ Falling back to HDFS/local snapshot or CSV files")
        tried = False
        if HDFS_URI:
            hdfs_snap = os.path.join(HDFS_URI.rstrip('/'), "data/transactions.parquet")
            try:
                df = spark.read.parquet(hdfs_snap)
                print(f"✓ Loaded snapshot from HDFS: {hdfs_snap}")
                tried = True
            except Exception as e:
                print(f"  Could not load HDFS snapshot: {e}")

        if df is None:
            local_snap = "data/transactions.parquet"
            if Path(local_snap).exists():
                try:
                    df = spark.read.parquet(local_snap)
                    print(f"✓ Loaded local snapshot: {local_snap}")
                    tried = True
                except Exception as e:
                    print(f"  Could not read local snapshot: {e}")

        if df is None:
            # Final fallback: CSVs in repository (development convenience)
            try:
                # Prefer `dataset/` directory for CSVs (original layout), otherwise `data/`
                def csv_path(fname: str) -> str:
                    for prefix in ("dataset", "data"):
                        p = os.path.join(prefix, fname)
                        if os.path.exists(p):
                            return p
                    return os.path.join("data", fname)

                train_csv = csv_path("fraudTrain.csv")
                test_csv = csv_path("fraudTest.csv")

                train_df = spark.read.csv(train_csv, header=True, inferSchema=True, enforceSchema=False)
                test_df = spark.read.csv(test_csv, header=True, inferSchema=True, enforceSchema=False)

                # Drop accidental unnamed columns
                if "_c0" in train_df.columns:
                    train_df = train_df.drop("_c0")
                if "_c0" in test_df.columns:
                    test_df = test_df.drop("_c0")

                df = train_df.union(test_df)
                print(f"✓ Loaded CSV fallback with {df.count():,} rows (from {train_csv} & {test_csv})")
                tried = True
            except Exception as e:
                print(f"✗ Failed to load CSV fallback: {e}")

        if not tried:
            raise RuntimeError("No training data available from Cassandra, snapshots, or CSVs")

    # ------------------------------------------------------------------
    # 2) Normalize schema: ensure numeric columns exist, impute categorical modes
    # ------------------------------------------------------------------
    print("\n[Feature Engineering] Normalizing columns and imputing where necessary...")

    # Canonicalize trans_time field
    if "trans_time" not in df.columns and "trans_date_trans_time" in df.columns:
        df = df.withColumn("trans_time", to_timestamp(col("trans_date_trans_time"), "yyyy-MM-dd HH:mm:ss"))

    # Protect dob/age
    if "dob" in df.columns:
        df = df.withColumn("dob_ts", to_timestamp(col("dob"), "yyyy-MM-dd"))
        df = df.withColumn("age", (year(current_date()) - year(col("dob_ts"))).cast(DoubleType()))
    else:
        df = df.withColumn("age", lit(30.0))

    # Numeric columns we expect
    numeric_cols = ["amt", "city_pop", "lat", "long", "merch_lat", "merch_long"]
    for ncol in numeric_cols:
        if ncol not in df.columns:
            df = df.withColumn(ncol, lit(0.0))
        else:
            df = df.withColumn(ncol, col(ncol).cast(DoubleType()))

    # Safe distance calculation (guard against nulls)
    df = df.withColumn(
        "distance",
        when(
            col("lat").isNotNull() & col("long").isNotNull() & col("merch_lat").isNotNull() & col("merch_long").isNotNull(),
            ((col("lat") - col("merch_lat")) ** 2 + (col("long") - col("merch_long")) ** 2) ** 0.5
        ).otherwise(lit(0.0)).cast(DoubleType())
    )

    # trans_hour
    if "trans_time" in df.columns:
        df = df.withColumn("trans_hour", hour(col("trans_time")).cast(DoubleType()))
    else:
        df = df.withColumn("trans_hour", lit(0.0))

    # Label
    if "is_fraud" in df.columns:
        df = df.withColumn("label", col("is_fraud").cast(DoubleType()))
    else:
        df = df.withColumn("label", lit(0.0))

    print("✓ Normalization complete")

    # ------------------------------------------------------------------
    # 3) Categorical handling: choose which categoricals to include
    # ------------------------------------------------------------------
    candidate_cats = ["category", "gender"]
    present_cats = []
    for c in candidate_cats:
        if c in df.columns:
            try:
                mode_row = df.groupBy(c).count().orderBy(col("count").desc()).limit(1).collect()
                if mode_row and mode_row[0][0] is not None:
                    mode_val = mode_row[0][0]
                    df = df.fillna({c: mode_val})
                    present_cats.append(c)
                    print(f"✓ Categorical '{c}' present; imputed nulls with mode='{mode_val}'")
                else:
                    print(f"→ Categorical '{c}' exists but has no values; excluding from pipeline")
            except Exception as e:
                print(f"⚠ Could not compute mode for {c}: {e}")
        else:
            print(f"→ Categorical '{c}' not present; excluding from pipeline")

    # ---------------------------------------------------------------
    # 4) Prepare features and pipeline dynamically
    # ---------------------------------------------------------------
    numeric_features = ["amt", "distance", "age", "trans_hour", "city_pop", "lat", "long"]
    indexers = []
    index_output_cols = []
    for c in present_cats:
        out = f"{c}_idx"
        # allow unseen categories at transform time; training imputed modes so report won't show 'unknown'
        indexers.append(StringIndexer(inputCol=c, outputCol=out, handleInvalid="keep"))
        index_output_cols.append(out)

    assembler_inputs = numeric_features + index_output_cols
    assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features_raw")
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True, withStd=True)

    rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=100, maxDepth=10, seed=42)

    stages = []
    stages.extend(indexers)
    stages.append(assembler)
    stages.append(scaler)
    stages.append(rf)

    pipeline = Pipeline(stages=stages)

    print("\n[Training] Built pipeline with: ")
    print(f"  Numeric features: {numeric_features}")
    print(f"  Categorical (included): {present_cats}")

    # ---------------------------------------------------------------
    # 5) Balance dataset (simple oversample of fraud)
    # ---------------------------------------------------------------
    fraud_df = df.filter(col("label") == 1)
    non_fraud_df = df.filter(col("label") == 0)

    fraud_count = fraud_df.count()
    non_fraud_count = non_fraud_df.count()
    print(f"\n[Balance] Fraud rows: {fraud_count:,} | Non-fraud rows: {non_fraud_count:,}")

    if non_fraud_count == 0:
        print("⚠ No non-fraud rows found; training aborted")
        spark.stop()
        return

    sampling_ratio = min(1.0, (fraud_count * 3) / non_fraud_count) if non_fraud_count > 0 else 1.0
    non_fraud_sampled = non_fraud_df.sample(False, sampling_ratio, seed=42)
    balanced_df = fraud_df.union(non_fraud_sampled)
    print(f"✓ Balanced dataset: {balanced_df.count():,} rows")

    # ---------------------------------------------------------------
    # 6) Train / Evaluate
    # ---------------------------------------------------------------
    print("\n[Train] Splitting and training model...")
    train_data, test_data = balanced_df.randomSplit([0.8, 0.2], seed=42)
    print(f"  Training rows: {train_data.count():,} | Test rows: {test_data.count():,}")

    model = pipeline.fit(train_data)
    print("✓ Model trained")

    predictions = model.transform(test_data)

    binary_eval = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
    auc = binary_eval.evaluate(predictions)

    multi_eval = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")
    accuracy = multi_eval.evaluate(predictions, {multi_eval.metricName: "accuracy"})
    precision = multi_eval.evaluate(predictions, {multi_eval.metricName: "weightedPrecision"})
    recall = multi_eval.evaluate(predictions, {multi_eval.metricName: "weightedRecall"})
    f1 = multi_eval.evaluate(predictions, {multi_eval.metricName: "f1"})

    confusion_df = predictions.groupBy("label", "prediction").count()
    confusion_rows = confusion_df.orderBy("label", "prediction").collect()

    # Feature importances (map back to feature names)
    rf_model = model.stages[-1]
    importances = rf_model.featureImportances.toArray()
    feature_names = numeric_features + index_output_cols
    top_features = sorted(zip(feature_names, importances), key=lambda x: -x[1])[:10]

    # ---------------------------------------------------------------
    # 7) Write human-readable training report
    # ---------------------------------------------------------------
    reports_dir = Path("reports")
    reports_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    report_path = reports_dir / f"spark_fraud_training_report_{timestamp}.txt"

    with report_path.open("w") as f:
        f.write("CREDIT CARD FRAUD DETECTION - TRAINING REPORT\n")
        f.write("=" * 70 + "\n\n")
        f.write(f"Timestamp: {timestamp}\n")
        f.write(f"Spark Version: {spark.version}\n\n")

        f.write("Dataset Summary\n")
        f.write("---------------\n")
        f.write(f"Total rows (balanced training DF): {balanced_df.count():,}\n")
        f.write(f"Fraud rows: {fraud_count:,}\n")
        f.write(f"Non-fraud rows (sampled): {non_fraud_sampled.count():,}\n\n")

        f.write("Performance Metrics\n")
        f.write("-------------------\n")
        f.write(f"Accuracy   : {accuracy*100:.4f}%\n")
        f.write(f"Precision  : {precision*100:.4f}%\n")
        f.write(f"Recall     : {recall*100:.4f}%\n")
        f.write(f"F1-Score   : {f1*100:.4f}%\n")
        f.write(f"AUC-ROC    : {auc*100:.4f}%\n\n")

        f.write("Confusion Matrix\n")
        f.write("----------------\n")
        for row in confusion_rows:
            f.write(f"{int(row['label'])}\t{int(row['prediction'])}\t{row['count']}\n")
        f.write("\n")

        f.write("Top Feature Importances\n")
        f.write("------------------------\n")
        for feat, imp in top_features:
            f.write(f"{feat:25s} : {imp:.6f}\n")
        f.write("\n")

        if present_cats:
            f.write("Categorical Summary (mode-imputed)\n")
            f.write("-------------------------------\n")
            for c in present_cats:
                vals = balanced_df.groupBy(c).count().orderBy(col("count").desc()).limit(10).collect()
                f.write(f"{c}: ")
                f.write(", ".join([f"{r[0]}({r[1]})" for r in vals if r[0] is not None]))
                f.write("\n")

    print(f"\n✓ Report saved to {report_path}")

    # ---------------------------------------------------------------
    # 8) Save model locally
    # ---------------------------------------------------------------
    model_path = "models/spark_fraud_model"
    Path("models").mkdir(exist_ok=True)
    try:
        model.write().overwrite().save(model_path)
        print(f"✓ Model saved to {model_path}")
    except Exception as e:
        try:
            model.write().overwrite().save(f"file://{os.getcwd()}/{model_path}")
            print(f"✓ Forced local model save succeeded: {model_path}")
        except Exception as err:
            print(f"✗ Failed to save model: {err}")

    print("\n" + "=" * 60)
    print("TRAINING COMPLETE")
    print("=" * 60 + "\n")

    spark.stop()


if __name__ == "__main__":
    main()
