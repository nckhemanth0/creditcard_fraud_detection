from kafka import KafkaProducer
import os
import json

broker = os.getenv('KAFKA_BROKER', 'localhost:9094')
print(f"Testing sending to: {broker}")

try:
    producer = KafkaProducer(
        bootstrap_servers=[broker], 
        request_timeout_ms=10000, 
        api_version=(2, 8, 1),
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    future = producer.send('test_topic', {'key': 'value'})
    result = future.get(timeout=10)
    print(f"Success! Sent to partition {result.partition} at offset {result.offset}")
    producer.close()
except Exception as e:
    print(f"Failed: {e}")
