"""
Kafka Transaction Producer - Simulates POS terminals sending transactions
Sends real-time credit card transactions to Kafka for Spark Streaming
"""
from kafka import KafkaProducer
from kafka.errors import KafkaError
import pandas as pd
import json
import time
import os
import random

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = 'creditcardTransaction'

def create_producer():
    """Create Kafka producer with retry logic"""
    max_retries = 5
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3
            )
            print(f"✓ Connected to Kafka broker: {KAFKA_BROKER}")
            return producer
        except KafkaError as e:
            print(f"  Attempt {attempt+1}/{max_retries}: Kafka not ready...")
            time.sleep(5)
    raise Exception("Failed to connect to Kafka")

def main():
    print("=" * 60)
    print("Kafka Transaction Producer (POS Terminal Simulator)")
    print("=" * 60)
    
    # Load test data
    print("\n[1/3] Loading test transactions...")
    try:
        df = pd.read_csv('dataset/fraudTest.csv')
        if 'Unnamed: 0' in df.columns:
            df = df.drop(columns=['Unnamed: 0'])
        print(f"✓ Loaded {len(df):,} transactions")
    except FileNotFoundError:
        print("Error: fraudTest.csv not found!")
        return
    
    # Create producer
    print("\n[2/3] Connecting to Kafka...")
    producer = create_producer()
    
    # Send transactions
    print(f"\n[3/3] Streaming transactions to topic '{KAFKA_TOPIC}'...")
    print("  Press Ctrl+C to stop\n")
    
    fraud_sent = 0
    normal_sent = 0
    
    try:
        for idx, row in df.iterrows():
            # Convert row to dict
            transaction = row.to_dict()
            
            # Convert any numpy types to Python types
            for key, value in transaction.items():
                if pd.isna(value):
                    transaction[key] = None
                elif hasattr(value, 'item'):
                    transaction[key] = value.item()
            
            # Send to Kafka
            future = producer.send(KAFKA_TOPIC, value=transaction)
            
            # Track counts
            if transaction.get('is_fraud') == 1:
                fraud_sent += 1
                print(f"  🚨 FRAUD: ${transaction['amt']:.2f} at {transaction['merchant'][:30]}")
            else:
                normal_sent += 1
            
            # Progress update every 100 transactions
            if (idx + 1) % 100 == 0:
                print(f"  Sent: {idx+1:,} | Normal: {normal_sent:,} | Fraud: {fraud_sent:,}")
            
            # Simulate real-time stream (100-300ms delay)
            time.sleep(random.uniform(0.1, 0.3))
            
    except KeyboardInterrupt:
        print("\n\nStopping producer...")
    finally:
        producer.flush()
        producer.close()
        print(f"\n✓ Producer stopped. Sent {normal_sent + fraud_sent:,} transactions")
        print(f"  Normal: {normal_sent:,} | Fraud: {fraud_sent:,}")

if __name__ == "__main__":
    main()
