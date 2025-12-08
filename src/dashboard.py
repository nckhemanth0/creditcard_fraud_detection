"""
PHASE B - Dashboard: Real-time fraud alert visualization
"""
from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO
from cassandra.cluster import Cluster
from datetime import datetime, timedelta
import threading
import time

app = Flask(__name__)
app.config['SECRET_KEY'] = 'fraud-detection-secret'
socketio = SocketIO(app, cors_allowed_origins="*")

# Cassandra connection
cluster = None
session = None

def get_cassandra_session():
    global cluster, session
    if session is None:
        cluster = Cluster(['localhost'], port=9042)
        session = cluster.connect('creditcard')
    return session

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/stats')
def get_stats():
    session = get_cassandra_session()
    
    fraud_count = session.execute("SELECT COUNT(*) FROM fraud_transaction").one()[0]
    non_fraud_count = session.execute("SELECT COUNT(*) FROM non_fraud_transaction").one()[0]
    customer_count = session.execute("SELECT COUNT(*) FROM customer").one()[0]
    
    return jsonify({
        'fraud_count': fraud_count,
        'non_fraud_count': non_fraud_count,
        'customer_count': customer_count,
        'fraud_rate': round(fraud_count / (fraud_count + non_fraud_count) * 100, 2) if (fraud_count + non_fraud_count) > 0 else 0
    })

@app.route('/api/recent_fraud')
def get_recent_fraud():
    session = get_cassandra_session()
    
    # Get recent fraud transactions
    rows = session.execute("""
        SELECT cc_num, trans_time, merchant, amt, category 
        FROM fraud_transaction 
        LIMIT 20
    """)
    
    fraud_list = []
    for row in rows:
        fraud_list.append({
            'cc_num': row.cc_num[-4:] if row.cc_num else '****',
            'time': row.trans_time.strftime('%Y-%m-%d %H:%M:%S') if row.trans_time else '',
            'merchant': row.merchant,
            'amount': float(row.amt),
            'category': row.category
        })
    
    return jsonify(fraud_list)

def fraud_monitor():
    """Background thread to monitor new fraud transactions"""
    session = get_cassandra_session()
    last_count = 0
    
    while True:
        try:
            current_count = session.execute("SELECT COUNT(*) FROM fraud_transaction").one()[0]
            if current_count > last_count:
                # New fraud detected
                socketio.emit('fraud_alert', {
                    'count': current_count,
                    'new': current_count - last_count
                })
                last_count = current_count
        except:
            pass
        time.sleep(2)

@socketio.on('connect')
def handle_connect():
    print('Client connected')

if __name__ == '__main__':
    # Start fraud monitor in background
    monitor_thread = threading.Thread(target=fraud_monitor, daemon=True)
    monitor_thread.start()
    
    print("=" * 50)
    print("PHASE B - Fraud Alert Dashboard")
    print("=" * 50)
    print("\n🌐 Dashboard running at: http://localhost:8080")
    print("Press Ctrl+C to stop\n")
    
    socketio.run(app, host='0.0.0.0', port=8080, debug=False, allow_unsafe_werkzeug=True)

