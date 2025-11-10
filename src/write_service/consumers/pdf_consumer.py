import json
import time
from sqlalchemy.exc import SQLAlchemyError
from confluent_kafka import Consumer, KafkaError
from .models import Base, engine, SessionLocal, get_or_create_table_class

def create_table_if_not_exists(table_name: str):
    """Create a table for a given site if missing."""
    DynamicTable = get_or_create_table_class(table_name)
    Base.metadata.create_all(bind=engine, tables=[DynamicTable.__table__])
    return DynamicTable

def insert_record(session, table_class, record):
    """Insert record into the site table."""
    new_row = table_class(
        pdf_id=record["pdf_id"],
        entities=record["entities"],
        snippet=record.get("snippet"),
        tables=record.get("tables", []),
    )
    session.add(new_row)

def main():
    topic = "processed-data-topic"
    site_table = "stlouis_gov_crime"  # could be derived dynamically per message
    table_class = create_table_if_not_exists(site_table)
    
    consumer = Consumer({
        "bootstrap.servers": "kafka:29092",
        "group.id": "pdf-processor-group",
        "auto.offset.reset": "earliest",
    })

    consumer.subscribe([topic])
    print(f"Listening on Kafka topic: {topic}")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Kafka error: {msg.error()}")
                    continue
            
            try:
                payload = json.loads(msg.value().decode("utf-8"))
                with SessionLocal() as session:
                    insert_record(session, table_class, payload)
                    session.commit()
                    print(f"Inserted PDF {payload['pdf_id']} into {site_table}")
            except SQLAlchemyError as e:
                print(f"Database error: {e}")
                session.rollback()
                time.sleep(3)
            except Exception as e:
                print(f"Unexpected error: {e}")
                time.sleep(3)
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
