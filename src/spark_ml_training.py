"""
PHASE A - Step 2: ML Model Training using Apache Spark MLlib
Uses Random Forest Classifier for distributed machine learning
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, year, current_date, to_timestamp, hour
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer, VectorAssembler, StandardScaler
)
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
import os
from datetime import datetime
from pathlib import Path


def main():
    print("=" * 60)
    print("PHASE A - Step 2: Spark MLlib Model Training")
    print("=" * 60)

    # ---------------------------------------------------------------
    # 1. Initialize Spark (Force LOCAL FS, disable HDFS completely)
    # ---------------------------------------------------------------
    print("\n[1/8] Initializing Apache Spark for ML...")

    spark = (
        SparkSession.builder
        .appName("FraudDetection-MLTraining")
        .master("local[*]")

        # >>> IMPORTANT FIX <<<
        # Stop Spark from using HDFS (localhost:9000)
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
        .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")

        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    print("✓ Spark Session initialized")
    print(f"  Spark Version: {spark.version}")

    # ---------------------------------------------------------------
    # 2. Load dataset (Fix empty _c0 column problem)
    # ---------------------------------------------------------------
    print("\n[2/8] Loading dataset with Spark...")

    train_df = (
        spark.read.csv(
            "dataset/fraudTrain.csv",
            header=True,
            inferSchema=True,
            enforceSchema=False
        )
        .drop("_c0")
    )

    test_df = (
        spark.read.csv(
            "dataset/fraudTest.csv",
            header=True,
            inferSchema=True,
            enforceSchema=False
        )
        .drop("_c0")
    )

    df = train_df.union(test_df)
    print(f"✓ Loaded {df.count():,} transactions")

    # ---------------------------------------------------------------
    # 3. Feature Engineering
    # ---------------------------------------------------------------
    print("\n[3/8] Feature Engineering with Spark SQL...")

    df = df.withColumn("trans_time", to_timestamp(col("trans_date_trans_time"), "yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("dob_ts", to_timestamp(col("dob"), "yyyy-MM-dd"))
    df = df.withColumn("age", (year(current_date()) - year(col("dob_ts"))).cast("double"))

    df = df.withColumn(
        "distance",
        ((col("lat") - col("merch_lat")) ** 2 + (col("long") - col("merch_long")) ** 2) ** 0.5
    )

    df = df.withColumn("trans_hour", hour(col("trans_time")).cast("double"))

    for colname in ["amt", "city_pop", "lat", "long", "merch_lat", "merch_long"]:
        df = df.withColumn(colname, col(colname).cast("double"))

    df = df.withColumn("label", col("is_fraud").cast("double"))

    print("✓ Features engineered")

    # Fraud distribution
    print("\n  Fraud Distribution:")
    df.groupBy("label").count().show()

    # ---------------------------------------------------------------
    # 4. Balance dataset
    # ---------------------------------------------------------------
    print("\n[4/8] Balancing dataset...")

    fraud_df = df.filter(col("label") == 1)
    non_fraud_df = df.filter(col("label") == 0)

    fraud_count = fraud_df.count()
    non_fraud_count = non_fraud_df.count()

    sampling_ratio = min(1.0, (fraud_count * 3) / non_fraud_count)
    non_fraud_sampled = non_fraud_df.sample(False, sampling_ratio, seed=42)

    balanced_df = fraud_df.union(non_fraud_sampled)

    print(f"✓ Balanced dataset: {balanced_df.count():,} rows")
    print(f"  Fraud: {fraud_count:,} | Sampled Non-Fraud: {non_fraud_sampled.count():,}")

    # ---------------------------------------------------------------
    # 5. ML Pipeline
    # ---------------------------------------------------------------
    print("\n[5/8] Building Spark ML Pipeline...")

    category_indexer = StringIndexer(inputCol="category", outputCol="category_idx", handleInvalid="keep")
    gender_indexer = StringIndexer(inputCol="gender", outputCol="gender_idx", handleInvalid="keep")

    numeric_features = ["amt", "distance", "age", "trans_hour", "city_pop", "lat", "long"]
    categorical_features = ["category_idx", "gender_idx"]

    assembler = VectorAssembler(
        inputCols=numeric_features + categorical_features,
        outputCol="features_raw",
    )

    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True, withStd=True)

    rf = RandomForestClassifier(
        labelCol="label",
        featuresCol="features",
        numTrees=100,
        maxDepth=10,
        seed=42
    )

    pipeline = Pipeline(stages=[
        category_indexer, gender_indexer, assembler, scaler, rf
    ])

    print("✓ Pipeline built")

    # ---------------------------------------------------------------
    # 6. Train model
    # ---------------------------------------------------------------
    print("\n[6/8] Training model...")
    train_data, test_data = balanced_df.randomSplit([0.8, 0.2], seed=42)

    print(f"  Training rows: {train_data.count():,}")
    print(f"  Test rows: {test_data.count():,}")

    model = pipeline.fit(train_data)
    print("✓ Model trained")

    # ---------------------------------------------------------------
    # 7. Evaluate model
    # ---------------------------------------------------------------
    print("\n[7/8] Evaluating model...")

    predictions = model.transform(test_data)

    binary_eval = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
    auc = binary_eval.evaluate(predictions)

    multi_eval = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")

    accuracy = multi_eval.evaluate(predictions, {multi_eval.metricName: "accuracy"})
    precision = multi_eval.evaluate(predictions, {multi_eval.metricName: "weightedPrecision"})
    recall = multi_eval.evaluate(predictions, {multi_eval.metricName: "weightedRecall"})
    f1 = multi_eval.evaluate(predictions, {multi_eval.metricName: "f1"})

    # Confusion matrix
    confusion_df = predictions.groupBy("label", "prediction").count()
    confusion_rows = confusion_df.orderBy("label", "prediction").collect()
    confusion_df.show()

    # Feature importance
    rf_model = model.stages[-1]
    importances = rf_model.featureImportances.toArray()
    feature_names = numeric_features + categorical_features

    top_features = sorted(zip(feature_names, importances), key=lambda x: -x[1])[:5]

    # ---------------------------------------------------------------
    # Write report
    # ---------------------------------------------------------------
    reports_dir = Path("reports")
    reports_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    report_path = reports_dir / f"spark_fraud_training_report_{timestamp}.txt"

    with report_path.open("w") as f:
        f.write("CREDIT CARD FRAUD DETECTION - ML TRAINING REPORT\n")
        f.write("=" * 70 + "\n\n")
        f.write(f"Timestamp: {timestamp}\n")
        f.write(f"Spark Version: {spark.version}\n\n")

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
            f.write(f"{feat:20s} : {imp:.6f}\n")

    print(f"\n✓ Report saved to {report_path}")

    # ---------------------------------------------------------------
    # 8. Save model (LOCAL ONLY, never HDFS)
    # ---------------------------------------------------------------
    print("\n[8/8] Saving model...")

    model_path = "models/spark_fraud_model"
    os.makedirs("models", exist_ok=True)

    try:
        model.write().overwrite().save(model_path)
        print(f"✓ Model saved to {model_path}")
    except Exception as e:
        print("⚠ Spark attempted HDFS save. Retrying with forced local path...")
        model.write().overwrite().save(f"file://{os.getcwd()}/{model_path}")
        print(f"✓ Forced local save succeeded")

    print("\n" + "=" * 60)
    print("✓ TRAINING COMPLETE")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
