"""
PHASE B - Dashboard: Real-time fraud alert visualization
"""
from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO
from cassandra.cluster import Cluster
from datetime import datetime
import threading
import time
from typing import Dict

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

    # small in-memory cache so repeated failures don't spam Cassandra
    CACHE_TTL = 10
    if not hasattr(estimate_table_rows, "_cache"):
        estimate_table_rows._cache: Dict[str, Dict[str, float]] = {}

    now = time.time()
    try:
        rows = session.execute(f"""
            SELECT partitions_count 
            FROM system.size_estimates
            WHERE keyspace_name='creditcard'
            AND table_name='{table}'
            ALLOW FILTERING;
        """)

        total = 0
        for r in rows:
            if getattr(r, 'partitions_count', None):
                total += r.partitions_count

        estimate_table_rows._cache[table] = {"ts": now, "value": total}
        return total
    except Exception as e:
        print("Monitor error:", e)
        cached = estimate_table_rows._cache.get(table)
        if cached and (now - cached.get("ts", 0) < CACHE_TTL):
            return cached.get("value", 0)
        return 0


@app.route('/api/diagnostics')
def get_diagnostics():
    """Return diagnostics: estimates, small samples and session info to help debugging."""
    session = get_cassandra_session()
    out = {"estimates": {}, "samples": {}, "session": {}}

    try:
        for t in ("fraud_transaction", "non_fraud_transaction", "customer", "fraud_alert"):
            try:
                est = estimate_table_rows(t)
            except Exception as e:
                est = None
            out["estimates"][t] = est

            # Try to fetch a tiny sample (limit 5). If table doesn't exist, capture exception.
            try:
                sample_rows = []
                rows = session.execute(f"SELECT * FROM {t} LIMIT 5")
                for r in rows:
                    # Convert Row to dict with safe attributes
                    sample_rows.append({k: (getattr(r, k) if hasattr(r, k) else None) for k in r._fields})
                out["samples"][t] = sample_rows
            except Exception as e:
                out["samples"][t] = {"error": str(e)}

        # Add basic session info
        try:
            out["session"]["hosts"] = [str(h.address) for h in session.cluster.contact_points]
        except Exception:
            out["session"]["hosts"] = []

    except Exception as e:
        return jsonify({"error": "Diagnostics failed", "details": str(e)}), 500

    return jsonify(out)


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
    try:
        # Prefer model alerts table; fallback to transactions if alerts not available
        try:
            rows = session.execute("""
                SELECT trans_num, cc_num, trans_time, merchant, amt, category
                FROM fraud_alert
                LIMIT 20
            """)
        except Exception:
            rows = session.execute("""
                SELECT trans_num, cc_num, trans_time, merchant, amt, category
                FROM fraud_transaction
                LIMIT 20
            """)

        fraud_list = []
        for row in rows:
            fraud_list.append({
                "trans_num": getattr(row, 'trans_num', '') or '',
                "cc_num": (getattr(row, 'cc_num', '') or '')[-4:] if getattr(row, 'cc_num', None) else "****",
                "time": row.trans_time.strftime('%Y-%m-%d %H:%M:%S') if getattr(row, 'trans_time', None) else "",
                "merchant": getattr(row, 'merchant', '') or "",
                "amount": float(getattr(row, 'amt', 0.0) or 0.0),
                "category": getattr(row, 'category', '') or ""
            })

        return jsonify(fraud_list)
    except Exception as e:
        return jsonify({"error": "Cassandra unavailable or timed out", "details": str(e)}), 503


# ------------------------------------------------------------
#   Background thread — monitors new fraud transactions
# ------------------------------------------------------------
def fraud_monitor():
    session = get_cassandra_session()
    backoff = 2

    # Track today's partition for fraud_alert (partition key is trans_day)
    def today_partition():
        return datetime.utcnow().strftime('%Y-%m-%d')

    trans_day = today_partition()
    try:
        # initialize last_count by counting today's partition (fast)
        rows = session.execute("SELECT COUNT(*) as c FROM fraud_alert WHERE trans_day=%s", (trans_day,))
        last_count = int(rows.one().c if rows.one() and getattr(rows.one(), 'c', None) is not None else 0)
    except Exception:
        last_count = 0

    while True:
        try:
            # refresh partition key in case day rolled over
            new_day = today_partition()
            if new_day != trans_day:
                trans_day = new_day
                try:
                    rows = session.execute("SELECT COUNT(*) as c FROM fraud_alert WHERE trans_day=%s", (trans_day,))
                    last_count = int(rows.one().c if rows.one() and getattr(rows.one(), 'c', None) is not None else 0)
                except Exception:
                    last_count = 0

            # count only within today's partition (avoids full-table scans)
            rows = session.execute("SELECT COUNT(*) as c FROM fraud_alert WHERE trans_day=%s", (trans_day,))
            current_count = int(rows.one().c if rows.one() and getattr(rows.one(), 'c', None) is not None else 0)

            if current_count > last_count:
                new_alerts = current_count - last_count

                # Fetch the most recent alerts for today's partition (limit 5)
                recent = []
                try:
                    q = "SELECT trans_num, cc_num, trans_time, merchant, amt, category FROM fraud_alert WHERE trans_day=%s LIMIT 5"
                    res = session.execute(q, (trans_day,))
                    for r in res:
                        recent.append({
                            'trans_num': getattr(r, 'trans_num', ''),
                            'cc_num': (getattr(r, 'cc_num', '') or '')[-4:],
                            'time': r.trans_time.strftime('%Y-%m-%d %H:%M:%S') if getattr(r, 'trans_time', None) else '',
                            'merchant': getattr(r, 'merchant', '') or '',
                            'amount': float(getattr(r, 'amt', 0.0) or 0.0),
                            'category': getattr(r, 'category', '') or ''
                        })
                except Exception:
                    recent = []

                socketio.emit("fraud_alert", {
                    "count": current_count,
                    "new": new_alerts,
                    "recent": recent
                })
                last_count = current_count

            backoff = 2
        except Exception as e:
            print("Monitor error:", e)
            backoff = min(backoff * 2, 60)

        time.sleep(backoff)


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
