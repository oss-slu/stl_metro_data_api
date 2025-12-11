import os
import csv
import time
import socket
import psycopg2
from psycopg2.extras import execute_batch
from kafka import KafkaProducer
import json

# Config
CSV_FILE = os.getenv("CRIME_CSV", "frontend/crime_data.csv")
TABLE_NAME = os.getenv("TABLE_NAME", "stlouis_gov_crime")

# Detect if running inside Docker
def is_docker_host(hostname="postgres", port=5432, timeout=1):
    try:
        sock = socket.create_connection((hostname, port), timeout=timeout)
        sock.close()
        return True
    except Exception:
        return False

# Postgres config
if is_docker_host():
    PG_HOST = "postgres"
    PG_PORT = 5432
else:
    PG_HOST = os.getenv("PG_HOST", "localhost")
    PG_PORT = int(os.getenv("PG_PORT", 5433))

PG_DB = os.getenv("PG_DB", "stl_data")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "Welcome123456")

# Kafka config
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
RAW_TOPIC = os.getenv("RAW_TOPIC", "raw-data-topic")

print(f"Connecting to Postgres at {PG_HOST}:{PG_PORT} db={PG_DB} user={PG_USER}")

# Connect to Postgres
conn = psycopg2.connect(
    host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD
)
cur = conn.cursor()

# Connect to Kafka
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Wait for Kafka broker
while True:
    try:
        if producer.bootstrap_connected():
            print(f"Connected to Kafka broker at {KAFKA_BROKER}")
            break
    except Exception as e:
        print("Kafka not ready, retrying in 5s...", e)
        time.sleep(5)

# Read CSV and process
with open(CSV_FILE, newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    headers = reader.fieldnames

    # Dynamically create table
    cols = [f'"{h}" TEXT' for h in headers]
    create_table_sql = f'CREATE TABLE IF NOT EXISTS "{TABLE_NAME}" (id SERIAL PRIMARY KEY, {", ".join(cols)});'
    cur.execute(create_table_sql)
    conn.commit()
    print(f"Table '{TABLE_NAME}' ensured in Postgres.")

    # Prepare batch insert
    columns = ', '.join(f'"{h}"' for h in headers)
    placeholders = ', '.join([f'%({h})s' for h in headers])
    insert_sql = f'INSERT INTO "{TABLE_NAME}" ({columns}) VALUES ({placeholders})'

    batch = []
    count = 0
    for row in reader:
        # send to Kafka
        producer.send(RAW_TOPIC, row)
        batch.append(row)
        count += 1

        # batch insert in chunks of 1000
        if len(batch) >= 1000:
            execute_batch(cur, insert_sql, batch)
            conn.commit()
            batch.clear()
            print(f"Inserted {count} rows so far...")

    # insert remaining rows
    if batch:
        execute_batch(cur, insert_sql, batch)
        conn.commit()
        print(f"Inserted total of {count} rows.")

producer.flush()
cur.close()
conn.close()
print(f"Finished sending {count} rows to topic '{RAW_TOPIC}' and inserting into '{TABLE_NAME}'")