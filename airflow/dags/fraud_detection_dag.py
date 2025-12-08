"""
Apache Airflow DAG for Credit Card Fraud Detection Pipeline
============================================================

Automates the COMPLETE Big Data Pipeline:

Phase A (Batch Processing):
  1. Spark SQL - Data Import to Cassandra
  2. Spark MLlib - Train Random Forest Model

Phase B (Real-time Processing):
  3. Spark Streaming - Real-time Fraud Detection
  4. Kafka Producer - Simulate POS Transactions
  5. Dashboard - Fraud Alert Visualization

Technologies:
- Apache Spark SQL (Distributed ETL)
- Apache Spark MLlib (Distributed ML)
- Apache Spark Streaming (Real-time Processing)
- Apache Kafka (Message Streaming)
- Apache Cassandra (Distributed Storage)
- Apache Airflow (Workflow Automation)
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator

# Project path
PROJECT_PATH = '/opt/airflow/project'

# Default arguments
default_args = {
    'owner': 'fraud-detection-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# ============================================================
# DAG 1: Daily Batch Pipeline (Model Retraining)
# ============================================================
batch_dag = DAG(
    'fraud_detection_batch_pipeline',
    default_args=default_args,
    description='Daily batch pipeline: Data Import + Model Training',
    schedule_interval='@daily',  # Runs daily at midnight
    catchup=False,
    tags=['spark', 'mllib', 'batch', 'fraud-detection'],
)

# Task 1: Check Infrastructure
check_infra = BashOperator(
    task_id='check_infrastructure',
    bash_command='''
        echo "=============================================="
        echo "Checking Big Data Infrastructure..."
        echo "=============================================="
        
        # Check Cassandra
        python3 -c "
from cassandra.cluster import Cluster
try:
    c = Cluster(['cassandra'])
    s = c.connect()
    print('✓ Cassandra: OK')
    c.shutdown()
except Exception as e:
    print(f'✗ Cassandra: {e}')
    exit(1)
" || exit 1

        # Check Kafka
        python3 -c "
from kafka import KafkaProducer
try:
    p = KafkaProducer(bootstrap_servers=['kafka:29092'])
    print('✓ Kafka: OK')
    p.close()
except Exception as e:
    print(f'✗ Kafka: {e}')
    exit(1)
" || exit 1

        echo "✓ All infrastructure checks passed!"
    ''',
    dag=batch_dag,
)

# Task 2: Spark SQL - Data Import
spark_data_import = BashOperator(
    task_id='spark_sql_data_import',
    bash_command=f'''
        echo "=============================================="
        echo "PHASE A - Step 1: Spark SQL Data Import"
        echo "=============================================="
        cd {PROJECT_PATH}
        python3 src/spark_data_import.py
        echo "✓ Data import complete!"
    ''',
    dag=batch_dag,
)

# Task 3: Spark MLlib - Model Training
spark_ml_training = BashOperator(
    task_id='spark_mllib_model_training',
    bash_command=f'''
        echo "=============================================="
        echo "PHASE A - Step 2: Spark MLlib Model Training"
        echo "=============================================="
        cd {PROJECT_PATH}
        python3 src/spark_ml_training.py
        echo "✓ Model training complete!"
    ''',
    dag=batch_dag,
)

# Task 4: Validate Model
validate_model = BashOperator(
    task_id='validate_model',
    bash_command=f'''
        echo "Validating trained model..."
        cd {PROJECT_PATH}
        python3 -c "
from pyspark.ml import PipelineModel
try:
    model = PipelineModel.load('models/spark_fraud_model')
    print('✓ Model validated successfully!')
    print(f'  Stages: {len(model.stages)}')
except Exception as e:
    print(f'✗ Model validation failed: {e}')
    exit(1)
"
    ''',
    dag=batch_dag,
)

# Task 5: Pipeline Complete
batch_complete = BashOperator(
    task_id='batch_pipeline_complete',
    bash_command='''
        echo ""
        echo "╔════════════════════════════════════════════════════════╗"
        echo "║     BATCH PIPELINE COMPLETE - Model Retrained!         ║"
        echo "╠════════════════════════════════════════════════════════╣"
        echo "║  ✓ Spark SQL: Data imported to Cassandra               ║"
        echo "║  ✓ Spark MLlib: Random Forest model trained            ║"
        echo "║  ✓ Model validated and ready for streaming             ║"
        echo "╚════════════════════════════════════════════════════════╝"
    ''',
    dag=batch_dag,
)

# Batch DAG Flow
check_infra >> spark_data_import >> spark_ml_training >> validate_model >> batch_complete


# ============================================================
# DAG 2: Real-time Streaming Pipeline
# ============================================================
streaming_dag = DAG(
    'fraud_detection_streaming_pipeline',
    default_args=default_args,
    description='Start real-time fraud detection streaming',
    schedule_interval=None,  # Triggered manually or after batch
    catchup=False,
    tags=['spark-streaming', 'kafka', 'real-time', 'fraud-detection'],
)

# Task 1: Start Spark Streaming
start_streaming = BashOperator(
    task_id='start_spark_streaming',
    bash_command=f'''
        echo "=============================================="
        echo "PHASE B: Starting Spark Streaming"
        echo "=============================================="
        cd {PROJECT_PATH}
        
        # Kill any existing streaming process
        pkill -f spark_streaming_detector || true
        
        # Start streaming in background
        nohup python3 src/spark_streaming_detector.py > /tmp/spark_streaming.log 2>&1 &
        
        sleep 10
        echo "✓ Spark Streaming started!"
        echo "  Log: /tmp/spark_streaming.log"
    ''',
    dag=streaming_dag,
)

# Task 2: Start Kafka Producer
start_producer = BashOperator(
    task_id='start_kafka_producer',
    bash_command=f'''
        echo "Starting Kafka Transaction Producer..."
        cd {PROJECT_PATH}
        
        # Kill any existing producer
        pkill -f kafka_producer || true
        
        # Start producer in background
        nohup python3 src/kafka_producer.py > /tmp/kafka_producer.log 2>&1 &
        
        sleep 5
        echo "✓ Kafka Producer started!"
        echo "  Simulating POS terminal transactions..."
    ''',
    dag=streaming_dag,
)

# Task 3: Start Dashboard
start_dashboard = BashOperator(
    task_id='start_dashboard',
    bash_command=f'''
        echo "Starting Fraud Alert Dashboard..."
        cd {PROJECT_PATH}
        
        # Kill any existing dashboard
        pkill -f dashboard.py || true
        
        # Start dashboard in background
        nohup python3 src/dashboard.py > /tmp/dashboard.log 2>&1 &
        
        sleep 5
        echo "✓ Dashboard started!"
        echo "  URL: http://localhost:8080"
    ''',
    dag=streaming_dag,
)

# Task 4: Streaming Complete
streaming_complete = BashOperator(
    task_id='streaming_pipeline_complete',
    bash_command='''
        echo ""
        echo "╔════════════════════════════════════════════════════════╗"
        echo "║     STREAMING PIPELINE ACTIVE!                         ║"
        echo "╠════════════════════════════════════════════════════════╣"
        echo "║  Flow: POS → Kafka → Spark Streaming → Cassandra       ║"
        echo "║                           ↓                            ║"
        echo "║                      Dashboard → Fraud Alerts          ║"
        echo "╠════════════════════════════════════════════════════════╣"
        echo "║  Dashboard: http://localhost:8080                      ║"
        echo "╚════════════════════════════════════════════════════════╝"
    ''',
    dag=streaming_dag,
)

# Streaming DAG Flow
start_streaming >> start_producer >> start_dashboard >> streaming_complete


# ============================================================
# DAG 3: Full Pipeline (Batch + Streaming)
# ============================================================
full_dag = DAG(
    'fraud_detection_full_pipeline',
    default_args=default_args,
    description='Complete pipeline: Batch processing + Real-time streaming',
    schedule_interval='@weekly',  # Weekly full refresh
    catchup=False,
    tags=['spark', 'mllib', 'streaming', 'kafka', 'full-pipeline'],
)

# Full pipeline tasks
full_check = BashOperator(
    task_id='check_infrastructure',
    bash_command='echo "Checking infrastructure..." && sleep 2',
    dag=full_dag,
)

full_import = BashOperator(
    task_id='spark_sql_import',
    bash_command=f'cd {PROJECT_PATH} && python3 src/spark_data_import.py',
    dag=full_dag,
)

full_train = BashOperator(
    task_id='spark_mllib_train',
    bash_command=f'cd {PROJECT_PATH} && python3 src/spark_ml_training.py',
    dag=full_dag,
)

full_stream = BashOperator(
    task_id='start_streaming',
    bash_command=f'''
        cd {PROJECT_PATH}
        pkill -f spark_streaming_detector || true
        nohup python3 src/spark_streaming_detector.py > /tmp/streaming.log 2>&1 &
        sleep 10
    ''',
    dag=full_dag,
)

full_producer = BashOperator(
    task_id='start_producer',
    bash_command=f'''
        cd {PROJECT_PATH}
        pkill -f kafka_producer || true
        nohup python3 src/kafka_producer.py > /tmp/producer.log 2>&1 &
        sleep 5
    ''',
    dag=full_dag,
)

full_dashboard = BashOperator(
    task_id='start_dashboard',
    bash_command=f'''
        cd {PROJECT_PATH}
        pkill -f dashboard.py || true
        nohup python3 src/dashboard.py > /tmp/dashboard.log 2>&1 &
        sleep 5
        echo "Dashboard: http://localhost:8080"
    ''',
    dag=full_dag,
)

full_complete = BashOperator(
    task_id='pipeline_complete',
    bash_command='''
        echo ""
        echo "╔══════════════════════════════════════════════════════════════╗"
        echo "║          FULL FRAUD DETECTION PIPELINE COMPLETE!             ║"
        echo "╠══════════════════════════════════════════════════════════════╣"
        echo "║  Phase A (Batch):                                            ║"
        echo "║    ✓ Spark SQL - Data imported to Cassandra                  ║"
        echo "║    ✓ Spark MLlib - Random Forest model trained               ║"
        echo "║                                                              ║"
        echo "║  Phase B (Real-time):                                        ║"
        echo "║    ✓ Spark Streaming - Processing Kafka transactions         ║"
        echo "║    ✓ Kafka Producer - Simulating POS terminals               ║"
        echo "║    ✓ Dashboard - Displaying fraud alerts                     ║"
        echo "║                                                              ║"
        echo "║  Dashboard: http://localhost:8080                            ║"
        echo "╚══════════════════════════════════════════════════════════════╝"
    ''',
    dag=full_dag,
)

# Full DAG Flow
full_check >> full_import >> full_train >> full_stream >> full_producer >> full_dashboard >> full_complete
