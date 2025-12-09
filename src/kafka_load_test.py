"""
KAFKA LOAD TEST - Stress test Kafka streaming throughput

Saves a JSON + text report under `reports/` with the achieved rate and errors.
Run: `python src/kafka_load_test.py --rate 100 --duration 30`
"""
import time
import os
import json
import argparse
from datetime import datetime


def csv_path(fname: str) -> str:
    for prefix in ("dataset", "data"):
        p = os.path.join(prefix, fname)
        if os.path.exists(p):
            return p
    return os.path.join("dataset", fname)


def main():
    parser = argparse.ArgumentParser(description="Kafka Load Test")
    parser.add_argument("--rate", type=int, default=100, help="Messages per second")
    parser.add_argument("--duration", type=int, default=30, help="Test duration in seconds")
    parser.add_argument("--topic", type=str, default=os.getenv('KAFKA_TOPIC', 'creditcardTransaction'))
    parser.add_argument("--broker", type=str, default=os.getenv('KAFKA_BROKER', 'localhost:9092'))
    args = parser.parse_args()

    try:
        import pandas as pd
        from kafka import KafkaProducer
    except Exception as e:
        print('Missing dependencies: pandas and kafka-python are required')
        print(e)
        return

    train_path = csv_path('fraudTrain.csv')
    test_path = csv_path('fraudTest.csv')
    sample_path = test_path if os.path.exists(test_path) else train_path

    print('=' * 60)
    print('KAFKA LOAD TEST - Stress Testing Streaming Pipeline')
    print('=' * 60)

    print('\n[1/3] Loading test data...')
    df = pd.read_csv(sample_path)
    if 'Unnamed: 0' in df.columns:
        df = df.drop(columns=['Unnamed: 0'])
    print(f'✓ Loaded {len(df):,} sample transactions')

    print('\n[2/3] Connecting to Kafka...')
    try:
        producer = KafkaProducer(
            bootstrap_servers=[args.broker],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks=1,
            linger_ms=5,
            batch_size=16384
        )
        print(f'✓ Connected to {args.broker}')
    except Exception as e:
        print(f'✗ Failed to connect: {e}')
        return

    print('\n[3/3] Running load test...')
    print(f'  Target rate: {args.rate} msgs/sec')
    print(f'  Duration: {args.duration} seconds')
    print('-' * 60)

    sent = 0
    errors = 0
    start_time = time.time()
    interval_start = start_time
    interval_sent = 0

    delay = 1.0 / max(1, args.rate)

    try:
        while time.time() - start_time < args.duration:
            row = df.iloc[sent % len(df)]
            transaction = {k: (None if pd.isna(v) else (v.item() if hasattr(v, 'item') else v)) for k, v in row.to_dict().items()}
            try:
                producer.send(args.topic, value=transaction)
                sent += 1
                interval_sent += 1
            except Exception:
                errors += 1

            if time.time() - interval_start >= 1.0:
                actual_rate = interval_sent / (time.time() - interval_start)
                print(f'  Rate: {actual_rate:.0f} msgs/sec | Total: {sent:,} | Errors: {errors}')
                interval_start = time.time()
                interval_sent = 0

            time.sleep(delay)

    except KeyboardInterrupt:
        print('\n\nStopping test...')
    finally:
        try:
            producer.flush()
            producer.close()
        except Exception:
            pass

    elapsed = time.time() - start_time
    actual_rate = sent / elapsed if elapsed > 0 else 0

    reports_dir = 'reports'
    os.makedirs(reports_dir, exist_ok=True)
    timestamp = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    json_path = os.path.join(reports_dir, f'loadtest_{timestamp}.json')
    txt_path = os.path.join(reports_dir, f'loadtest_{timestamp}.txt')

    report = {
        'timestamp': timestamp,
        'target_rate': args.rate,
        'duration': args.duration,
        'messages_sent': sent,
        'errors': errors,
        'actual_rate': actual_rate,
        'broker': args.broker,
        'topic': args.topic,
    }

    with open(json_path, 'w') as fh:
        json.dump(report, fh, indent=2)

    with open(txt_path, 'w') as fh:
        fh.write('KAFKA LOAD TEST REPORT\n')
        fh.write('=' * 60 + '\n')
        fh.write(f"Duration: {elapsed:.1f}s\nMessages Sent: {sent:,}\nErrors: {errors}\nActual Rate: {actual_rate:.1f} msgs/sec\n")

    print('\nLoad test complete. Reports saved:')
    print(' -', json_path)
    print(' -', txt_path)


if __name__ == '__main__':
    main()
