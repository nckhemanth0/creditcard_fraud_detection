CREATE KEYSPACE IF NOT EXISTS creditcard
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE creditcard;

CREATE TABLE IF NOT EXISTS customer (
  cc_num text PRIMARY KEY,
  first text,
  last text,
  gender text,
  street text,
  city text,
  state text,
  zip text,
  lat double,
  long double,
  job text,
  dob timestamp
);

CREATE TABLE IF NOT EXISTS fraud_transaction (
  cc_num text,
  trans_time timestamp,
  trans_num text,
  category text,
  merchant text,
  amt double,
  merch_lat double,
  merch_long double,
  distance double,
  age int,
  is_fraud double,
  PRIMARY KEY (cc_num, trans_time)
) WITH CLUSTERING ORDER BY (trans_time DESC);

CREATE TABLE IF NOT EXISTS non_fraud_transaction (
  cc_num text,
  trans_time timestamp,
  trans_num text,
  category text,
  merchant text,
  amt double,
  merch_lat double,
  merch_long double,
  distance double,
  age int,
  is_fraud double,
  PRIMARY KEY (cc_num, trans_time)
) WITH CLUSTERING ORDER BY (trans_time DESC);

CREATE TABLE IF NOT EXISTS kafka_offset (
  partition int PRIMARY KEY,
  offset bigint
);

-- Table to store fraud alerts for dashboard/monitoring.
-- Partition by day to keep partitions bounded and allow efficient "recent" queries.
CREATE TABLE IF NOT EXISTS fraud_alert (
  trans_day text,
  trans_time timestamp,
  trans_num text,
  cc_num text,
  merchant text,
  category text,
  amt double,
  is_fraud double,
  prediction_score double,
  PRIMARY KEY ((trans_day), trans_time, trans_num)
) WITH CLUSTERING ORDER BY (trans_time DESC);

