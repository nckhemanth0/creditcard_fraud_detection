# Architecture Flow Analysis

## ✅ **ARCHITECTURE vs CODE COMPARISON**

### **Expected Flow (from architecture.png):**

```
1. INITIAL DATA (CSV) → Spark SQL → Cassandra
2. INITIAL DATA (CSV) → MODEL (Random Forest)
3. Kafka → Spark Streaming → Spark MLlib (uses MODEL) → Cassandra
4. Cassandra → Dashboard (Flask) → FRAUD ALERT
```

---

### **Actual Code Flow:**

#### **1. Data Import (`spark_data_import.py`)**
```55:78:src/spark_data_import.py
    train_df = spark.read.csv("dataset/fraudTrain.csv", header=True, inferSchema=True)
    test_df = spark.read.csv("dataset/fraudTest.csv", header=True, inferSchema=True)
```
- ✅ **Source**: CSV files (`dataset/fraudTrain.csv`, `dataset/fraudTest.csv`)
- ✅ **Processing**: Spark SQL (ETL, feature engineering)
- ✅ **Destination**: Cassandra (`fraud_transaction`, `non_fraud_transaction`, `customer` tables)
- ✅ **Matches Architecture**: INITIAL DATA → Spark SQL → Cassandra

---

#### **2. ML Training (`spark_ml_training.py`)**
```55:75:src/spark_ml_training.py
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
```
- ✅ **Source**: CSV files (`dataset/fraudTrain.csv`, `dataset/fraudTest.csv`) - **NOT Cassandra!**
- ✅ **Processing**: Spark MLlib (Random Forest Classifier)
- ✅ **Output**: Saved model to `models/spark_fraud_model`
- ✅ **Matches Architecture**: INITIAL DATA → MODEL

---

#### **3. Real-time Detection (`spark_streaming_detector.py`)**
```112:120:src/spark_streaming_detector.py
    # Load trained Spark ML model
    print("\n[2/5] Loading Spark ML model...")
    try:
        model = PipelineModel.load("models/spark_fraud_model")
        print("✓ Spark ML model loaded (Random Forest)")
        use_ml_model = True
    except Exception as e:
        print(f"⚠ Model not found, using is_fraud label directly: {e}")
        use_ml_model = False
```
```127:133:src/spark_streaming_detector.py
    kafka_df = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load())
```
```196:202:src/spark_streaming_detector.py
                try:
                    fraud_alerts.write \
                        .format("org.apache.spark.sql.cassandra") \
                        .options(table="fraud_alert", keyspace="creditcard") \
                        .mode("append") \
                        .save()
                    print(f"  ✓ Saved {fraud_count} alerts to Cassandra")
```
- ✅ **Source**: Kafka (`creditcardTransaction` topic)
- ✅ **Processing**: Spark Streaming → Spark MLlib (loads trained model)
- ✅ **Destination**: Cassandra (`fraud_alert` table)
- ✅ **Matches Architecture**: Kafka → Spark Streaming → Spark MLlib → Cassandra

---

#### **4. Dashboard (`dashboard.py`)**
```86:107:src/dashboard.py
@app.route('/api/recent_fraud')
def get_recent_fraud():
    session = get_cassandra_session()

    # Recent frauds (LIMIT 20)
    rows = session.execute("""
        SELECT cc_num, trans_time, merchant, amt, category
        FROM fraud_transaction
        LIMIT 20
    """)

    fraud_list = []
    for row in rows:
        fraud_list.append({
            "cc_num": row.cc_num[-4:] if row.cc_num else "****",
            "time": row.trans_time.strftime('%Y-%m-%d %H:%M:%S') if row.trans_time else "",
            "merchant": row.merchant,
            "amount": float(row.amt),
            "category": row.category
        })

    return jsonify(fraud_list)
```
- ✅ **Source**: Cassandra (`fraud_transaction`, `fraud_alert` tables)
- ✅ **Processing**: Flask + SocketIO (real-time updates)
- ✅ **Output**: Web dashboard with fraud alerts
- ✅ **Matches Architecture**: Cassandra → Dashboard → FRAUD ALERT

---

## 🎯 **ANSWER TO YOUR QUESTIONS:**

### **Q1: Is the architecture and our code the same?**
**✅ YES!** The code flow exactly matches the architecture diagram:
- ✅ CSV → Spark SQL → Cassandra
- ✅ CSV → ML Training → Model
- ✅ Kafka → Spark Streaming → ML Model → Cassandra
- ✅ Cassandra → Dashboard → Alerts

---

### **Q2: Is ML using Cassandra or initial data?**
**✅ ML Training uses INITIAL DATA (CSV files), NOT Cassandra!**

**Evidence:**
- `spark_ml_training.py` reads directly from `dataset/fraudTrain.csv` and `dataset/fraudTest.csv`
- It does **NOT** read from Cassandra tables
- This matches the architecture diagram which shows: **INITIAL DATA → MODEL**

**Why?**
- Training needs the full historical dataset with labels
- Cassandra is used for:
  - Storing processed transactions (for dashboard queries)
  - Storing real-time fraud alerts (from streaming)
  - **NOT** for ML training data

---

## 📊 **COMPLETE DATA FLOW:**

```
┌─────────────────┐
│  CSV Files      │
│  (Initial Data) │
└────────┬────────┘
         │
         ├─────────────────┐
         │                 │
         ▼                 ▼
┌─────────────────┐  ┌─────────────────┐
│ spark_data_     │  │ spark_ml_       │
│ import.py       │  │ training.py     │
│ (Spark SQL)     │  │ (Spark MLlib)   │
└────────┬────────┘  └────────┬────────┘
         │                    │
         ▼                    ▼
┌─────────────────┐  ┌─────────────────┐
│   Cassandra     │  │  ML Model       │
│   (Storage)     │  │  (Random Forest)│
└────────┬────────┘  └────────┬────────┘
         │                    │
         │                    │
         │         ┌──────────┴──────────┐
         │         │                     │
         │         ▼                     │
         │  ┌─────────────────┐          │
         │  │  Kafka         │          │
         │  │  (Real-time)   │          │
         │  └────────┬───────┘          │
         │           │                  │
         │           ▼                  │
         │  ┌─────────────────┐         │
         │  │ spark_streaming_│         │
         │  │ detector.py     │◄────────┘
         │  │ (Spark Streaming│
         │  │  + ML Model)    │
         │  └────────┬────────┘
         │           │
         │           ▼
         │  ┌─────────────────┐
         │  │   Cassandra     │
         │  │  (fraud_alert)  │
         │  └────────┬────────┘
         │           │
         │           ▼
         │  ┌─────────────────┐
         │  │   Dashboard     │
         │  │   (Flask)       │
         │  └─────────────────┘
```

---

## ✅ **CONCLUSION:**

**Everything matches the architecture!** The code correctly:
1. Uses CSV files for initial data import and ML training
2. Uses Cassandra for storing processed data and real-time alerts
3. Uses Kafka for real-time transaction streaming
4. Uses Spark (SQL, MLlib, Streaming) for all Big Data processing
5. Uses Flask dashboard for visualization

**No changes needed!** 🎉

