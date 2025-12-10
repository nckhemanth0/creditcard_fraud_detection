"""
DEMO SPIKE - Direct Cassandra Injection
Bypasses Kafka to directly insert fraud alerts for real-time dashboard demo
"""
from cassandra.cluster import Cluster
from datetime import datetime
import time
import random

def inject_fraud_spike():
    print("🚨 INJECTING FRAUD SPIKE - DIRECT TO CASSANDRA...")
    
    # Connect to Cassandra
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect('creditcard')
    
    # Prepare insert statement
    insert_query = """
        INSERT INTO fraud_alert (trans_day, trans_time, trans_num, cc_num, merchant, category, amt, is_fraud, prediction_score)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    prepared = session.prepare(insert_query)
    
    merchants = [
        "SUSPICIOUS_CRYPTO_EXCHANGE",
        "OFFSHORE_LUXURY_GOODS", 
        "UNAUTHORIZED_WIRE_TRANSFER",
        "DARK_WEB_MARKETPLACE",
        "FRAUDULENT_CASINO",
        "FAKE_JEWELRY_STORE",
        "SCAM_ELECTRONICS",
        "STOLEN_CARD_PURCHASE"
    ]
    
    categories = ["shopping_net", "misc_net", "personal_care", "entertainment"]
    
    # Inject 25 fraud transactions over 10 seconds
    for i in range(25):
        now = datetime.now()
        trans_day = now.strftime('%Y-%m-%d')
        trans_time = now
        trans_num = f"DEMO-FRAUD-{int(time.time())}-{i}"
        cc_num = f"{random.randint(1000, 9999):04d}"
        merchant = random.choice(merchants)
        category = random.choice(categories)
        amt = random.uniform(500, 15000)
        
        session.execute(prepared, (
            trans_day,
            trans_time,
            trans_num,
            cc_num,
            merchant,
            category,
            amt,
            1.0,
            0.98
        ))
        
        print(f"  ⚠️  FRAUD #{i+1}: ${amt:.2f} at {merchant}")
        time.sleep(0.4)  # 400ms between injections
    
    print(f"\n✅ SPIKE COMPLETE! {25} fraud alerts injected.")
    print("📊 Check dashboard at http://localhost:8080")
    
    cluster.shutdown()

if __name__ == "__main__":
    inject_fraud_spike()
