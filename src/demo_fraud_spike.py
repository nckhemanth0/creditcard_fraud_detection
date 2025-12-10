"""
Demo Script: Fraud Spike Generator
Injects a burst of blatantly fraudulent transactions to trigger alarms.
"""
from kafka import KafkaProducer
import json
import time
import random
import os
from datetime import datetime

KAFKA_BROKER = os.getenv('KAFKA_BROKER', '127.0.0.1:9094')
TOPIC = 'creditcardTransaction'

def main():
    print("WARNING: INJECTING FRAUD SPIKE...")
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(2, 8, 1),
        request_timeout_ms=10000,
        max_block_ms=10000
    )

    # Generate 20 high-value fraud transactions
    locations = [
        (40.7128, -74.0060, "New York, NY"), 
        (34.0522, -118.2437, "Los Angeles, CA"),
        (51.5074, -0.1278, "London, UK"),
        (35.6762, 139.6503, "Tokyo, JP"),
        (55.7558, 37.6173, "Moscow, RU")
    ]

    for i in range(20):
        lat, long, city = random.choice(locations)
        tx = {
            "trans_num": f"DEMO-{int(time.time())}-{i}",
            "trans_date_trans_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "cc_num": f"4{random.randint(100000000000000, 999999999999999)}",
            "merchant": f"HACKER_{random.choice(['PAYPAL', 'AMAZON', 'APPLE', 'ROLEX'])}_DEMO",
            "category": "shopping_net",
            "amt": random.uniform(5000.0, 25000.0), # HUGE amounts
            "first": "Demo",
            "last": "User",
            "gender": "M",
            "street": "123 Fraud St",
            "city": city,
            "state": "XX",
            "zip": "00000",
            "lat": lat - 0.1,
            "long": long - 0.1,
            "city_pop": 1000000,
            "job": "Fraudster",
            "dob": "1990-01-01",
            "merch_lat": lat,
            "merch_long": long,
            "is_fraud": 1
        }
        
        producer.send(TOPIC, value=tx)
        print(f"  -> Sent FRAUD: ${tx['amt']:.2f} at {tx['merchant']}")
        time.sleep(0.05)

    producer.flush()
    print("\n✓ SPIKE INJECTED! Check Dashboard.")

if __name__ == "__main__":
    main()
