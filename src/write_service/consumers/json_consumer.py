"""
json_consumer.py
This code takes the data from Kafka and saves it into the database.
To test the JSON consumer, JSON processor, and JSON fetcher, you 
must start up the Docker containers and then either
1) run: python -m src.write_service.consumers.json_consumer
OR
2) go to https://localhost:5000/json
"""
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json, time
import os
from sqlalchemy import Column, Integer, DateTime, JSON, String, Boolean, create_engine
from sqlalchemy.orm import Session, DeclarativeBase
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime, timezone
import urllib.parse
from src.write_service.ingestion.json_fetcher import get_json
from src.write_service.processing.json_processor import send_data

# Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
# Code will choose between two different hosts: 
# localhost (for local runs) and postgres (when running in Docker)
PG_HOST = os.getenv('PG_HOST', 'localhost,postgres')
PG_PORT = os.getenv('PG_PORT', '5433')
PG_DB = os.getenv('PG_DB', 'stl_data')
PG_USER = os.getenv('PG_USER', 'postgres')
PG_PASSWORD = os.getenv('PG_PASSWORD', "Welcome@123456") # update with pg password if needed

# SQL Alchemy requires this base class thing
class Base(DeclarativeBase):
    pass

def get_table_class(table_name):
    """
    This will create a table class if it doesn't exist, otherwise it will just return the table class.
    This allows the table to be named after the topic (e.g. crime, traffic, etc.)
    """
    class DataTable(Base):
        __tablename__ = table_name
        id = Column(Integer, primary_key=True, autoincrement=True)
        name = Column(String)
        content = Column(JSON, nullable=False)
        is_active = Column(Boolean)
        data_posted_on = Column(DateTime, default=datetime.now(timezone.utc))
    return DataTable

def retrieve_from_kafka(topic_name):
    """
    This function retrieves the JSON data from Kafka.
    """

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
            except NoBrokersAvailable:
                # Connection failed, try again
                print(f"Kafka consumer attempt {attempt+1} failed (NoBrokersAvailable), retrying in 5s...")
                time.sleep(5)
    except Exception as e:
        print(f"Something went wrong with Kafka Consumer!: \n {e}")
        raise

def save_into_database(data, topic_name, topic_extended_name=None):
    """
    This function takes the data then saves it into the PostgreSQL database into a table with the topic name
    using SQL Alchemy.
    Each entity of the data is a row.
    """

    if (topic_extended_name is None):
        topic_extended_name = topic_name
    
    try:
        # Ensure data is in right format (list) else abort!
        if (not isinstance(data, list)):
            print("The data to be saved into the database is not valid! It must be a list.")
            return
    
        # Connect to database
        encoded_password = urllib.parse.quote_plus(PG_PASSWORD)
        engine_url = f"postgresql://{PG_USER}:{encoded_password}@{PG_HOST}:{PG_PORT}/{PG_DB}"
        engine = create_engine(engine_url, echo=True)

        # Get and create the table (if it did not already exist)
        table = get_table_class(topic_name)
        Base.metadata.create_all(engine)

        # Now let's add stuff to the table
        session = Session(engine)
        with Session(engine) as session:

            # Each entity will be a new row
            entity_counter = 1

            for entity in data:
                # Ensure data is in JSON format, otherwise we skip it
                try:
                    json.dumps(entity)
                except json.JSONDecodeError:
                    print("Entity " + str(entity) + " is not valid JSON. Going to the next entity.")
                    continue
                
                new_row = table(
                    name = topic_extended_name + " Entity #" + str(entity_counter),
                    content = entity,
                    is_active = True)
                session.add(new_row)
                entity_counter += 1
                    
            # Push all changes
            session.commit()             

        # We are done!
        # Close the engine (or return the session if in test mode)
        if (str(engine.url) != "sqlite:///:memory:"):
            engine.dispose()
        else:
            return session

        print("PostgreSQL: OK")
        
    # Exceptions
    except SQLAlchemyError as e:
        print("An error occured when saving to the database. \n " + str(e))

    except Exception as e:
        print("An error occured when connecting to the database. \n " + str(e))

# Test function that retrieves real City data (ARPA funds), parses it, sends to Kafka, 
# retrieves from Kafka, and saves into the database
if __name__ == "__main__":
    testURL = "https://www.stlouis-mo.gov/customcf/endpoints/arpa/expenditures.cfm?format=json"
    result = get_json(testURL)
    kafka_status = send_data(result, "arpa")
    test_data = retrieve_from_kafka("arpa")
    save_into_database(test_data, "ARPA_funds", "ARPA funds usage")
    