from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json, time
import logging

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
    return

# Test function
if __name__ == "__main__":
    retrieve_from_kafka("JSON-data")