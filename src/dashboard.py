"""
PHASE B - Dashboard: Real-time fraud alert visualization
"""
from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO
from cassandra.cluster import Cluster
from datetime import datetime
import threading
import time

app = Flask(__name__)
app.config['SECRET_KEY'] = 'fraud-detection-secret'
socketio = SocketIO(app, cors_allowed_origins="*")

# Cassandra connection
cluster = None
session = None

# ------------------------------------------------------------
#   Helper: Cassandra session
# ------------------------------------------------------------
def get_cassandra_session():
    global cluster, session
    if session is None:
        cluster = Cluster(['localhost'], port=9042)
        session = cluster.connect('creditcard')
    return session

# ------------------------------------------------------------
#   FAST ROW COUNT (NO COUNT(*)) using system.size_estimates
# ------------------------------------------------------------
def estimate_table_rows(table):
    """
    Returns an approximate row count without full table scan.
    Prevents Cassandra read timeouts.
    """
    session = get_cassandra_session()

    rows = session.execute(f"""
        SELECT partitions_count 
        FROM system.size_estimates
        WHERE keyspace_name='creditcard'
        AND table_name='{table}'
        ALLOW FILTERING;
    """)

    total = 0
    for r in rows:
        if r.partitions_count:
            total += r.partitions_count

    return total


# ------------------------------------------------------------
#   Routes
# ------------------------------------------------------------
@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/stats')
def get_stats():
    session = get_cassandra_session()

    try:
        fraud_count = estimate_table_rows("fraud_transaction")
        non_fraud_count = estimate_table_rows("non_fraud_transaction")
        customer_count = estimate_table_rows("customer")

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    total = fraud_count + non_fraud_count
    fraud_rate = round((fraud_count / total) * 100, 2) if total > 0 else 0

    return jsonify({
        "fraud_count": fraud_count,
        "non_fraud_count": non_fraud_count,
        "customer_count": customer_count,
        "fraud_rate": fraud_rate
    })


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


# ------------------------------------------------------------
#   Background thread — monitors new fraud transactions
# ------------------------------------------------------------
def fraud_monitor():
    session = get_cassandra_session()
    last_count = estimate_table_rows("fraud_transaction")

    while True:
        try:
            current_count = estimate_table_rows("fraud_transaction")

            if current_count > last_count:
                socketio.emit("fraud_alert", {
                    "count": current_count,
                    "new": current_count - last_count
                })
                last_count = current_count

        except Exception as e:
            print("Monitor error:", e)

        time.sleep(2)


@socketio.on('connect')
def handle_connect():
    print("Client connected")


# ------------------------------------------------------------
#   Main app run
# ------------------------------------------------------------
if __name__ == "__main__":
    # Start background monitoring thread
    monitor_thread = threading.Thread(target=fraud_monitor, daemon=True)
    monitor_thread.start()

    print("=" * 50)
    print("PHASE B - Fraud Alert Dashboard")
    print("=" * 50)
    print("\n🌐 Dashboard running at: http://localhost:8080")
    print("Press Ctrl+C to stop\n")

    socketio.run(app, host="0.0.0.0", port=8080, debug=False)
