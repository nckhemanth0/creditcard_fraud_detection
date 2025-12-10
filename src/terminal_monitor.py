import os
import json
import time
from datetime import datetime
from collections import deque
from kafka import KafkaConsumer
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.console import Console
from rich import box

# Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', '127.0.0.1:9094')
TOPIC = 'creditcardTransaction'

console = Console()

def create_layout():
    layout = Layout()
    layout.split(
        Layout(name="header", size=3),
        Layout(name="main", ratio=1),
    )
    layout["main"].split_row(
        Layout(name="stats", ratio=1),
        Layout(name="log", ratio=2),
    )
    return layout

def generate_stats_table(total, fps, fraud_count):
    table = Table(box=box.SIMPLE)
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="bold white")
    
    table.add_row("Total Transactions", f"{total:,}")
    table.add_row("Transactions/Sec", f"{fps:.1f}")
    table.add_row("Fraud Detected (Session)", f"[red]{fraud_count:,}[/red]")
    table.add_row("Kafka Broker", KAFKA_BROKER)
    
    return Panel(
        table,
        title="[bold blue]Live Statistics[/bold blue]",
        border_style="blue",
    )

def generate_log_table(transactions):
    table = Table(box=box.SIMPLE_HEAD, expand=True)
    table.add_column("Time", style="dim", width=10)
    table.add_column("Card", style="yellow", width=8)
    table.add_column("Amount", justify="right", width=12)
    table.add_column("Merchant")
    table.add_column("Status", width=10)

    for tx in transactions:
        is_fraud = tx.get('is_fraud', 0) == 1.0
        status = "[red]FRAUD[/red]" if is_fraud else "[green]Safe[/green]"
        amt_color = "red" if is_fraud else "white"
        
        table.add_row(
            datetime.now().strftime("%H:%M:%S"),
            f"*{tx.get('cc_num', '0000')[-4:]}",
            f"[{amt_color}]${tx.get('amt', 0):.2f}[/{amt_color}]",
            tx.get('merchant', 'Unknown')[:20],
            status
        )

    return Panel(
        table,
        title="[bold yellow]Transaction Stream[/bold yellow]",
        border_style="yellow",
    )

def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        api_version=(2, 8, 1)
    )

    total_processed = 0
    fraud_count = 0
    start_time = time.time()
    recent_transactions = deque(maxlen=15)
    
    layout = create_layout()
    layout["header"].update(
        Panel("Credit Card Fraud Detection - Realtime Monitor", style="bold white on blue")
    )

    with Live(layout, refresh_per_second=4, screen=True) as live:
        while True:
            # Poll for new messages (non-blockingish)
            msg_batch = consumer.poll(timeout_ms=100)
            
            for tp, messages in msg_batch.items():
                for msg in messages:
                    tx = msg.value
                    total_processed += 1
                    if tx.get('is_fraud', 0) == 1.0:
                        fraud_count += 1
                    recent_transactions.appendleft(tx)

            # Update stats
            elapsed = time.time() - start_time
            fps = total_processed / elapsed if elapsed > 0 else 0
            
            layout["stats"].update(generate_stats_table(total_processed, fps, fraud_count))
            layout["log"].update(generate_log_table(recent_transactions))

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nMonitor stopped.")
