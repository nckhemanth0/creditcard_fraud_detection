"""
PHASE A - Step 2: ML Model Training using Apache Spark MLlib
Uses Random Forest Classifier for distributed machine learning
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, year, current_date, to_timestamp
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer, VectorAssembler, StandardScaler,
    OneHotEncoder
)
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.mllib.evaluation import MulticlassMetrics
import os

def main():
    print("=" * 60)
    print("PHASE A - Step 2: Spark MLlib Model Training")
    print("=" * 60)
    
    # Initialize Spark Session
    print("\n[1/8] Initializing Apache Spark for ML...")
    spark = SparkSession.builder \
        .appName("FraudDetection-MLTraining") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print("✓ Spark Session initialized for MLlib")
    print(f"  Spark Version: {spark.version}")
    
    # Load data
    print("\n[2/8] Loading dataset with Spark...")
    train_df = spark.read.csv("dataset/fraudTrain.csv", header=True, inferSchema=True)
    test_df = spark.read.csv("dataset/fraudTest.csv", header=True, inferSchema=True)
    
    # Combine for training (we'll do our own split)
    df = train_df.union(test_df)
    print(f"✓ Loaded {df.count():,} transactions")
    
    # Feature Engineering using Spark SQL
    print("\n[3/8] Feature Engineering with Spark SQL...")
    
    # Drop unnecessary columns
    if "Unnamed: 0" in df.columns:
        df = df.drop("Unnamed: 0")
    
    # Parse timestamp and extract features
    df = df.withColumn("trans_time", to_timestamp(col("trans_date_trans_time"), "yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("dob_ts", to_timestamp(col("dob"), "yyyy-MM-dd"))
    df = df.withColumn("age", (year(current_date()) - year(col("dob_ts"))).cast("double"))
    
    # Calculate distance (simplified using Euclidean for speed)
    df = df.withColumn(
        "distance",
        ((col("lat") - col("merch_lat")) ** 2 + (col("long") - col("merch_long")) ** 2) ** 0.5
    )
    
    # Extract hour from transaction time
    from pyspark.sql.functions import hour
    df = df.withColumn("trans_hour", hour(col("trans_time")).cast("double"))
    
    # Cast numeric columns
    df = df.withColumn("amt", col("amt").cast("double"))
    df = df.withColumn("city_pop", col("city_pop").cast("double"))
    df = df.withColumn("lat", col("lat").cast("double"))
    df = df.withColumn("long", col("long").cast("double"))
    df = df.withColumn("merch_lat", col("merch_lat").cast("double"))
    df = df.withColumn("merch_long", col("merch_long").cast("double"))
    df = df.withColumn("label", col("is_fraud").cast("double"))
    
    print("✓ Features engineered")
    
    # Show fraud distribution
    print("\n  Fraud Distribution:")
    df.groupBy("label").count().show()
    
    # Handle class imbalance using undersampling
    print("\n[4/8] Balancing dataset for Spark MLlib...")
    fraud_df = df.filter(col("label") == 1)
    non_fraud_df = df.filter(col("label") == 0)
    
    fraud_count = fraud_df.count()
    non_fraud_count = non_fraud_df.count()
    
    # Undersample non-fraud to balance (or use ratio)
    sampling_ratio = min(1.0, (fraud_count * 3) / non_fraud_count)  # 3:1 ratio
    non_fraud_sampled = non_fraud_df.sample(False, sampling_ratio, seed=42)
    
    balanced_df = fraud_df.union(non_fraud_sampled)
    print(f"✓ Balanced dataset: {balanced_df.count():,} records")
    print(f"  Fraud: {fraud_count:,}, Non-Fraud (sampled): {non_fraud_sampled.count():,}")
    
    # Build ML Pipeline
    print("\n[5/8] Building Spark ML Pipeline...")
    
    # Index categorical columns
    category_indexer = StringIndexer(inputCol="category", outputCol="category_idx", handleInvalid="keep")
    gender_indexer = StringIndexer(inputCol="gender", outputCol="gender_idx", handleInvalid="keep")
    
    # Numerical features
    numeric_features = ["amt", "distance", "age", "trans_hour", "city_pop", "lat", "long"]
    categorical_features = ["category_idx", "gender_idx"]
    
    # Assemble features
    assembler = VectorAssembler(
        inputCols=numeric_features + categorical_features,
        outputCol="features_raw",
        handleInvalid="skip"
    )
    
    # Scale features
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)
    
    # Random Forest Classifier (distributed)
    rf = RandomForestClassifier(
        labelCol="label",
        featuresCol="features",
        numTrees=100,
        maxDepth=10,
        seed=42
    )
    
    # Build pipeline
    pipeline = Pipeline(stages=[
        category_indexer,
        gender_indexer,
        assembler,
        scaler,
        rf
    ])
    
    print("✓ ML Pipeline created with stages:")
    print("  1. StringIndexer (category)")
    print("  2. StringIndexer (gender)")
    print("  3. VectorAssembler")
    print("  4. StandardScaler")
    print("  5. RandomForestClassifier (100 trees)")
    
    # Split data
    print("\n[6/8] Training Random Forest with Spark MLlib...")
    train_data, test_data = balanced_df.randomSplit([0.8, 0.2], seed=42)
    print(f"  Training set: {train_data.count():,}")
    print(f"  Test set: {test_data.count():,}")
    
    # Train model
    print("\n  Training distributed Random Forest model...")
    model = pipeline.fit(train_data)
    print("✓ Model trained!")
    
    # Evaluate model
    print("\n[7/8] Evaluating model performance...")
    predictions = model.transform(test_data)
    
    # Binary classification metrics
    binary_evaluator = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC"
    )
    auc = binary_evaluator.evaluate(predictions)
    
    # Multiclass metrics
    multi_evaluator = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction"
    )
    
    accuracy = multi_evaluator.evaluate(predictions, {multi_evaluator.metricName: "accuracy"})
    precision = multi_evaluator.evaluate(predictions, {multi_evaluator.metricName: "weightedPrecision"})
    recall = multi_evaluator.evaluate(predictions, {multi_evaluator.metricName: "weightedRecall"})
    f1 = multi_evaluator.evaluate(predictions, {multi_evaluator.metricName: "f1"})
    
    print("\n  ╔════════════════════════════════════════╗")
    print("  ║     SPARK MLLIB MODEL PERFORMANCE      ║")
    print("  ╠════════════════════════════════════════╣")
    print(f"  ║  Accuracy:  {accuracy*100:6.2f}%                   ║")
    print(f"  ║  Precision: {precision*100:6.2f}%                   ║")
    print(f"  ║  Recall:    {recall*100:6.2f}%                   ║")
    print(f"  ║  F1-Score:  {f1*100:6.2f}%                   ║")
    print(f"  ║  AUC-ROC:   {auc*100:6.2f}%                   ║")
    print("  ╚════════════════════════════════════════╝")
    
    # Confusion Matrix
    print("\n  Confusion Matrix:")
    predictions.groupBy("label", "prediction").count().show()
    
    # Feature Importance (from Random Forest)
    print("\n  Feature Importance (Random Forest):")
    rf_model = model.stages[-1]
    feature_names = numeric_features + categorical_features
    importances = rf_model.featureImportances.toArray()
    for name, importance in sorted(zip(feature_names, importances), key=lambda x: -x[1])[:5]:
        print(f"    {name}: {importance:.4f}")
    
    # Save model
    print("\n[8/8] Saving Spark ML model...")
    os.makedirs("models", exist_ok=True)
    model.write().overwrite().save("models/spark_fraud_model")
    print("✓ Model saved to: models/spark_fraud_model")
    
    print("\n" + "=" * 60)
    print("✓ SPARK MLLIB TRAINING COMPLETE!")
    print(f"  Algorithm: Random Forest (Distributed)")
    print(f"  Trees: 100 | Max Depth: 10")
    print(f"  Accuracy: {accuracy*100:.2f}%")
    print("=" * 60)
    
    spark.stop()

if __name__ == "__main__":
    main()

