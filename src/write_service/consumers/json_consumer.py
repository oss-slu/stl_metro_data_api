from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json, time
import logging
import os
import time

# Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
PG_HOST = os.getenv('PG_HOST', 'localhost')
PG_PORT = os.getenv('PG_PORT', '5432')
PG_DB = os.getenv('PG_DB', 'stl_data')
PG_USER = os.getenv('PG_USER', 'postgres')

def retrieve_from_kafka(topic_name):
    """
    This function retrieves the JSON data from Kafka.
    """
    if (topic_name == None):
        topic_name = "JSON-data"

    received_data = []

    try:
        # Since Kafka is super slow, let's give it 3 tries
        for attempt in range(3):
            try:
                # Connect to Kafka
                consumer = KafkaConsumer(
                    topic_name,
                    bootstrap_servers=['localhost:9092', 'kafka:29092'],
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                    auto_offset_reset='earliest',
                    consumer_timeout_ms=5000 
                )
                
                # Get all messages
                try:
                    for message in consumer:
                        data = message.value
                        received_data.append(data)
                        print(f"Received message: {data}")
                except Exception as e:
                    print(f"Database error: {e}")
                
                consumer.close()
                return received_data
                break
            except NoBrokersAvailable:
                # Connection failed, try again
                print(f"Kafka consumer attempt {attempt+1} failed (NoBrokersAvailable), retrying in 5s...")
                time.sleep(5)
    except Exception as e:
        print(f"Something went wrong with Kafka Consumer!: \n {e}")
        raise

def save_into_database(data):
    # I am still working on this!
    try:
        from sqlalchemy import create_engine, text
        
        # Connect to database
        engine_url = f"postgresql+psycopg2://{PG_USER}:@{PG_HOST}:{PG_PORT}/{PG_DB}"
        
        engine = create_engine(engine_url)
        
        # Test connection
        with engine.connect() as conn:
            result = conn.execute(text("SELECT * FROM snapshots"))
            print("Pulling rows?: " + str(result.returns_rows))
        
        engine.dispose()
        print("PostgreSQL: OK")
        
    # Exceptions
    except Exception as e:
        print(f"PostgreSQL failed: {e}")
        if "password authentication failed" in str(e).lower():
            print("Tip: Check password in .env and Docker logs. Try 'docker-compose down -v && docker-compose up -d' to reset.")
        elif "could not translate host name" in str(e).lower():
            print("Tip: This is usually due to special chars in password; encoding should fix it. Verify PG is running: docker ps")
        raise
    return

# Test function
if __name__ == "__main__":
    retrieve_from_kafka("JSON-data")
    save_into_database("")