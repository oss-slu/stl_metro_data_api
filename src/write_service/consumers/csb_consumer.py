"""
CSB Data Consumer: Consumes messages from Kafka and stores in PostgreSQL.

Responsibility: Read from Kafka, write to database.
Input: Messages from Kafka topic 'csb-service-requests'.
Output: Records in PostgreSQL stlouis_business_citizen table.
"""

import json
import logging
from datetime import datetime, timezone
from kafka import KafkaConsumer
from sqlalchemy.orm import sessionmaker
from models import get_db_engine, CSBServiceRequest

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'csb-service-requests'
KAFKA_GROUP_ID = 'csb-postgres-writer'

def create_consumer():
    """Create Kafka consumer."""
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )

def consume_csb_data():
    """
    Consume CSB service request messages from Kafka and store in PostgreSQL.
    """
    engine = get_db_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    
    consumer = create_consumer()
    logger.info(f"✓ Consumer connected. Listening on topic: {KAFKA_TOPIC}")
    
    batch = []
    BATCH_SIZE = 1000
    inserted_count = 0
    
    try:
        for message in consumer:
            try:
                data = message.value
                
                # Parse posted date
                data_posted_on = None
                if data.get('data_posted_on'):
                    data_posted_on = datetime.fromisoformat(data['data_posted_on'])
                
                # Create database record
                record = CSBServiceRequest(
                    created_on=datetime.now(timezone.utc),
                    data_posted_on=data_posted_on,
                    is_active=data['is_active'],
                    service_name=data['service_name'][:255] if data.get('service_name') else 'Unknown',
                    description=data.get('description', '')[:1000] if data.get('description') else '',
                    contact_info=data.get('contact_info', {}),
                    source_url=data.get('source_url', 'https://www.stlouis-mo.gov/data/datasets/dataset.cfm?id=5'),
                    raw_json=data.get('raw_data', {})
                )
                
                batch.append(record)
                inserted_count += 1
                
                # Bulk insert in batches
                if len(batch) >= BATCH_SIZE:
                    session.bulk_save_objects(batch)
                    session.commit()
                    batch = []
                    logger.info(f"Inserted {inserted_count:,} records...")
                    
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                continue
                
    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
    finally:
        # Insert remaining records
        if batch:
            session.bulk_save_objects(batch)
            session.commit()
        
        session.close()
        consumer.close()
        logger.info(f"✓ Total records inserted: {inserted_count:,}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    consume_csb_data()