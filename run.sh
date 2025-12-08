#!/bin/bash

# ============================================================
# Credit Card Fraud Detection - Big Data Pipeline
# Technologies: Apache Spark, Spark MLlib, Kafka, Cassandra
# ============================================================

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_banner() {
    echo ""
    echo "╔════════════════════════════════════════════════════════════╗"
    echo "║     Credit Card Fraud Detection - Big Data Pipeline        ║"
    echo "║     Spark SQL | Spark MLlib | Spark Streaming | Kafka      ║"
    echo "╚════════════════════════════════════════════════════════════╝"
    echo ""
}

case "$1" in
  start)
    print_banner
    echo -e "${BLUE}Starting Big Data infrastructure...${NC}"
    docker-compose up -d cassandra zookeeper kafka spark-master spark-worker
    echo ""
    echo "Waiting for services to initialize..."
    sleep 45
    echo ""
    echo -e "${GREEN}Creating Cassandra schema...${NC}"
    docker cp schema.cql fraud-cassandra:/tmp/schema.cql
    docker exec fraud-cassandra cqlsh -f /tmp/schema.cql 2>/dev/null || true
    echo ""
    echo -e "${GREEN}✓ Infrastructure ready!${NC}"
    docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep fraud
    echo ""
    echo "Access Points:"
    echo "  • Spark Master UI: http://localhost:8081"
    echo "  • Cassandra: localhost:9042"
    echo "  • Kafka: localhost:9092"
    ;;
    
  stop)
    echo -e "${YELLOW}Stopping all services...${NC}"
    docker-compose down
    echo -e "${GREEN}✓ Services stopped${NC}"
    ;;
    
  import)
    print_banner
    echo -e "${BLUE}[SPARK SQL] Running distributed data import...${NC}"
    python src/spark_data_import.py
    ;;
    
  train)
    print_banner
    echo -e "${BLUE}[SPARK MLLIB] Training Random Forest model...${NC}"
    python src/spark_ml_training.py
    ;;
    
  stream)
    print_banner
    echo -e "${BLUE}[SPARK STREAMING] Starting real-time fraud detection...${NC}"
    python src/spark_streaming_detector.py
    ;;
    
  producer)
    print_banner
    echo -e "${BLUE}[KAFKA] Starting transaction producer...${NC}"
    python src/kafka_producer.py
    ;;
    
  dashboard)
    print_banner
    echo -e "${BLUE}Starting Flask dashboard...${NC}"
    python src/dashboard.py
    ;;
    
  airflow)
    echo -e "${BLUE}Starting Apache Airflow...${NC}"
    docker-compose up -d postgres airflow-webserver airflow-scheduler
    echo "Waiting for Airflow to initialize (60s)..."
    sleep 60
    echo ""
    echo -e "${GREEN}✓ Airflow started!${NC}"
    echo "  URL: http://localhost:8090"
    echo "  Username: admin"
    echo "  Password: admin"
    ;;
    
  spark-submit)
    print_banner
    echo -e "${BLUE}Submitting job to Spark cluster...${NC}"
    docker exec fraud-spark-master spark-submit \
      --master spark://spark-master:7077 \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 \
      /app/src/$2
    ;;
    
  pipeline)
    print_banner
    echo -e "${BLUE}Running complete Big Data pipeline...${NC}"
    echo ""
    
    echo "Step 1/3: Spark SQL - Data Import"
    python src/spark_data_import.py
    echo ""
    
    echo "Step 2/3: Spark MLlib - Model Training"
    python src/spark_ml_training.py
    echo ""
    
    echo "Step 3/3: Ready for Streaming"
    echo ""
    echo -e "${GREEN}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║              BIG DATA PIPELINE COMPLETE!                   ║${NC}"
    echo -e "${GREEN}╠════════════════════════════════════════════════════════════╣${NC}"
    echo -e "${GREEN}║  To test real-time fraud detection:                        ║${NC}"
    echo -e "${GREEN}║    Terminal 1: ./run.sh stream    (Spark Streaming)        ║${NC}"
    echo -e "${GREEN}║    Terminal 2: ./run.sh producer  (Kafka Producer)         ║${NC}"
    echo -e "${GREEN}║    Terminal 3: ./run.sh dashboard (Fraud Dashboard)        ║${NC}"
    echo -e "${GREEN}╚════════════════════════════════════════════════════════════╝${NC}"
    ;;
    
  full)
    print_banner
    echo -e "${BLUE}Starting FULL Big Data architecture...${NC}"
    $0 start
    sleep 10
    $0 airflow
    echo ""
    echo -e "${GREEN}✓ Full architecture running!${NC}"
    docker ps --format "table {{.Names}}\t{{.Status}}" | grep fraud
    echo ""
    echo "Access Points:"
    echo "  • Spark Master: http://localhost:8081"
    echo "  • Airflow:      http://localhost:8090 (admin/admin)"
    echo "  • Cassandra:    localhost:9042"
    echo "  • Kafka:        localhost:9092"
    ;;
    
  status)
    echo ""
    echo "=== Docker Services ==="
    docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep -E "(fraud|NAMES)"
    echo ""
    echo "=== Cassandra Data ==="
    docker exec fraud-cassandra cqlsh -e "SELECT COUNT(*) FROM creditcard.fraud_transaction;" 2>/dev/null || echo "Cassandra not ready"
    echo ""
    echo "=== Spark Model ==="
    ls -la models/ 2>/dev/null || echo "No model trained yet"
    ;;
    
  clean)
    echo -e "${YELLOW}Cleaning all data...${NC}"
    docker-compose down -v
    rm -rf models/ data/ __pycache__/
    echo -e "${GREEN}✓ Cleaned${NC}"
    ;;
    
  *)
    print_banner
    echo "Usage: $0 {command}"
    echo ""
    echo "Infrastructure Commands:"
    echo "  start     - Start Cassandra, Kafka, Spark cluster"
    echo "  stop      - Stop all Docker services"
    echo "  status    - Show service status"
    echo "  clean     - Remove all data and containers"
    echo ""
    echo "Big Data Pipeline:"
    echo "  import    - [Spark SQL] Import data to Cassandra"
    echo "  train     - [Spark MLlib] Train Random Forest model"
    echo "  stream    - [Spark Streaming] Real-time fraud detection"
    echo "  producer  - [Kafka] Simulate POS transactions"
    echo "  dashboard - Start Flask fraud alert dashboard"
    echo ""
    echo "Automation:"
    echo "  airflow   - Start Apache Airflow (port 8090)"
    echo "  pipeline  - Run complete ETL + ML pipeline"
    echo "  full      - Start entire architecture"
    echo ""
    echo "Example workflow:"
    echo "  1. ./run.sh start     # Start infrastructure"
    echo "  2. ./run.sh pipeline  # Run Spark ETL + ML"
    echo "  3. ./run.sh stream    # Start Spark Streaming (Terminal 1)"
    echo "  4. ./run.sh producer  # Start Kafka Producer (Terminal 2)"
    echo "  5. ./run.sh dashboard # View fraud alerts (Terminal 3)"
    ;;
esac
