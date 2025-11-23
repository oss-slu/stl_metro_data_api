# src/write_service/consumers/web_consumer.py
"""
Kafka consumer for web-scraped data.

Reads from 'processed.web.data' topic and stores in PostgreSQL.
"""

import os
from kafka import KafkaConsumer
import json
import logging
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from .models import StLouisCensusData, get_db_engine, create_tables
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def retry_database_operation(func, max_retries=3, delay=2):
    """
    Retry database operations on failure
    func: function to retry
    max_retries: maximum number of retry attempts
    delay: seconds to wait between retries
    """
    for attempt in range(max_retries):
        try:
            return func()
        except SQLAlchemyError as e:
            if attempt == max_retries - 1:
                raise
            logger.warning(f"Database error (attempt {attempt + 1}/{max_retries}): {e}")
            time.sleep(delay)

def consume_web_data(topic="processed.web.data", max_messages=None, group_id=None):
    """Consume messages from Kafka and store in PostgreSQL"""
    # 1. Setup database
    engine = get_db_engine()
    create_tables(engine)
    Session = sessionmaker(bind=engine)
    # 2. Setup Kafka consumer
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

    # Use a transient consumer (no group) by default in tests to avoid committed-offsets.
    # Pass group_id explicitly if you want group behavior.
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=group_id,
        consumer_timeout_ms=10000
    )
    logger.info(f"Started consuming from topic: {topic}")

    # 3. Consume messages
    message_count = 0
    try:
        for message in consumer:
            data = message.value
            logger.info(f"Received message: {data}")
            # 4. Store in database
            session = Session()
            try:
                def insert_record():
                    record = StLouisCensusData(raw_json=data)
                    session.add(record)
                    session.commit()
                    return record
                
                retry_database_operation(insert_record)
                logger.info(f"Inserted record into database: {data}")

                message_count += 1
                if max_messages and message_count >= max_messages:
                    logger.info(f"reached max_messages({max_messages}), stopping")
                    break
            except Exception as e:
                session.rollback()
                logger.error(f"Failed to insert after retries: {e}")
                # TODO: Send to dead-letter queue
            finally:
                session.close()
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
    finally:
        consumer.close()

if __name__ == "__main__":
    # Run consumer
    consume_web_data()